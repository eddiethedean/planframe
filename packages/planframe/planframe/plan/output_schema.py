"""Derive output :class:`~planframe.schema.ir.Schema` for a :class:`~planframe.plan.nodes.PlanNode` subtree.

Used by :func:`~planframe.execution.execute_plan` so expressions compile against the schema of
each node's **input** rows (the output of ``node.prev``), not the final frame schema.
"""

from __future__ import annotations

from typing import Any, cast

from planframe.backend.errors import PlanFrameSchemaError
from planframe.expr.api import AggExpr, Expr, infer_dtype
from planframe.plan.nodes import (
    Agg,
    Cast,
    ConcatHorizontal,
    ConcatVertical,
    Drop,
    DropNulls,
    DropNullsAll,
    Duplicated,
    DynamicGroupByAgg,
    Explode,
    FillNull,
    Filter,
    GroupBy,
    Head,
    Hint,
    Join,
    JoinKeyColumn,
    Melt,
    Pivot,
    Posexplode,
    Project,
    Rename,
    RollingAgg,
    Sample,
    Select,
    Slice,
    Sort,
    Source,
    Tail,
    Unique,
    Unnest,
    WithColumn,
    WithRowCount,
)
from planframe.schema.ir import Field, Schema, collect_col_names_in_expr
from planframe.schema.source import schema_from_type


def plan_output_schema(node: Any) -> Schema:
    """Return the row schema produced after executing *node* (this step's output)."""

    if isinstance(node, Source):
        return schema_from_type(node.schema_type)

    if isinstance(node, Agg):
        gb = node.prev
        if not isinstance(gb, GroupBy):
            raise PlanFrameSchemaError("Agg must follow GroupBy")
        return _agg_output_schema(gb, node.named_aggs)

    if isinstance(node, DynamicGroupByAgg):
        return _dynamic_agg_output_schema(node)

    s_in = plan_output_schema(node.prev)
    return _apply_unary_step(s_in, node)


def _agg_output_schema(gb: GroupBy, named_aggs: dict[str, tuple[str, str] | Expr[Any]]) -> Schema:
    base = plan_output_schema(gb.prev)
    out_fields: list[Field] = []
    for i, k in enumerate(gb.keys):
        if isinstance(k, JoinKeyColumn):
            out_fields.append(base.get(k.name))
        else:
            out_fields.append(Field(name=f"__pf_g{i}", dtype=infer_dtype(k.expr)))

    fm = base.field_map()
    for out_name, spec in named_aggs.items():
        if (
            isinstance(spec, tuple)
            and len(spec) == 2
            and isinstance(spec[0], str)
            and isinstance(spec[1], str)
        ):
            op = spec[0]
            col = spec[1]
            base.get(col)
            dtype: object = object
            if op in {"count", "n_unique"}:
                dtype = int
            out_fields.append(Field(name=out_name, dtype=dtype))
        elif isinstance(spec, AggExpr):
            missing = collect_col_names_in_expr(spec.inner).difference(fm.keys())
            if missing:
                raise PlanFrameSchemaError(
                    f"aggregation expression references unknown columns: {sorted(missing)}"
                )
            out_fields.append(Field(name=out_name, dtype=infer_dtype(spec)))
        else:
            raise PlanFrameSchemaError(
                "agg expects (op, column_name) tuples or agg_sum/agg_mean/...(...) "
                f"over an expression, got {type(spec).__name__!r}"
            )

    return Schema(fields=tuple(out_fields))


def _dynamic_agg_output_schema(node: DynamicGroupByAgg) -> Schema:
    base = plan_output_schema(node.prev)
    out_fields: list[Field] = []
    out_fields.append(base.get(node.index_column))
    if node.by is not None:
        for c in node.by:
            out_fields.append(base.get(c))

    fm = base.field_map()
    for out_name, spec in node.named_aggs.items():
        if (
            isinstance(spec, tuple)
            and len(spec) == 2
            and isinstance(spec[0], str)
            and isinstance(spec[1], str)
        ):
            op = spec[0]
            col = spec[1]
            base.get(col)
            dtype: object = object
            if op in {"count", "n_unique"}:
                dtype = int
            out_fields.append(Field(name=out_name, dtype=dtype))
        elif isinstance(spec, AggExpr):
            missing = collect_col_names_in_expr(spec.inner).difference(fm.keys())
            if missing:
                raise PlanFrameSchemaError(
                    f"aggregation expression references unknown columns: {sorted(missing)}"
                )
            out_fields.append(Field(name=out_name, dtype=infer_dtype(spec)))
        else:
            raise PlanFrameSchemaError(
                "agg expects (op, column_name) tuples or agg_sum/agg_mean/...(...) "
                f"over an expression, got {type(spec).__name__!r}"
            )

    return Schema(fields=tuple(out_fields))


def _apply_unary_step(s_in: Schema, node: Any) -> Schema:
    if isinstance(
        node,
        (
            Filter,
            Sort,
            Unique,
            DropNulls,
            DropNullsAll,
            FillNull,
            Slice,
            Head,
            Tail,
            ConcatVertical,
            Sample,
            Hint,
        ),
    ):
        if isinstance(node, Unique):
            if node.subset is not None:
                s_in.select(node.subset)
            return s_in.unique()
        if isinstance(node, FillNull):
            if node.subset is not None:
                s_in.select(node.subset)
            return s_in.fill_null()
        if isinstance(node, DropNulls):
            if node.subset is not None:
                s_in.select(node.subset)
            return s_in.drop_nulls()
        if isinstance(node, DropNullsAll):
            if node.subset is not None:
                s_in.select(node.subset)
            return s_in.drop_nulls_all()
        return s_in

    if isinstance(node, Select):
        return s_in.select(node.columns)

    if isinstance(node, Project):
        return s_in.project(node.items)

    if isinstance(node, Drop):
        return s_in.drop(node.columns, strict=node.strict)

    if isinstance(node, Rename):
        return s_in.rename(node.mapping, strict=node.strict)

    if isinstance(node, WithColumn):
        dtype = infer_dtype(node.expr)
        return s_in.with_column(node.name, dtype=dtype)

    if isinstance(node, Cast):
        return s_in.cast(node.name, node.dtype)

    if isinstance(node, WithRowCount):
        return s_in.with_row_count(node.name)

    if isinstance(node, Duplicated):
        if node.subset is not None:
            s_in.select(node.subset)
        return s_in.duplicated(out_name=node.out_name)

    if isinstance(node, GroupBy):
        return s_in

    if isinstance(node, RollingAgg):
        dtype: object = object
        if node.op in {"count"}:
            dtype = int
        return s_in.with_column(node.out_name, dtype=dtype)

    if isinstance(node, Melt):
        return s_in.melt(
            id_vars=node.id_vars,
            value_vars=node.value_vars,
            variable_name=node.variable_name,
            value_name=node.value_name,
        )

    if isinstance(node, Join):
        right = cast(Any, node.right)
        right_schema = getattr(right, "_schema", None)
        if not isinstance(right_schema, Schema):
            raise PlanFrameSchemaError("Join right frame must expose a PlanFrame Schema as _schema")
        if node.how == "cross":
            return s_in.join_merge_cross(right_schema, suffix=node.suffix)
        return s_in.join_merge(
            right_schema,
            left_on=node.left_keys,
            right_on=node.right_keys,
            suffix=node.suffix,
        )

    if isinstance(node, ConcatHorizontal):
        other = cast(Any, node.other)
        other_schema = getattr(other, "_schema", None)
        if not isinstance(other_schema, Schema):
            raise PlanFrameSchemaError(
                "ConcatHorizontal other frame must expose a PlanFrame Schema as _schema"
            )
        return Schema(fields=tuple([*s_in.fields, *other_schema.fields]))

    if isinstance(node, Pivot):
        idx = tuple(node.index)
        if not idx:
            raise PlanFrameSchemaError("pivot requires non-empty index")
        s_in.select(idx)
        on_name = node.on
        s_in.get(on_name)
        value_cols = node.values
        if not value_cols:
            raise PlanFrameSchemaError("pivot requires non-empty values")
        for v in value_cols:
            s_in.get(v)

        on_columns = node.on_columns
        if on_columns is not None and node.sort_columns:
            on_columns = tuple(sorted(on_columns))

        out_fields: list[Field] = [s_in.get(c) for c in idx]
        if on_columns is not None:
            if len(value_cols) == 1:
                for c in on_columns:
                    out_fields.append(Field(name=str(c), dtype=object))
            else:
                for v in value_cols:
                    for c in on_columns:
                        out_fields.append(Field(name=f"{v}{node.separator}{c}", dtype=object))
        return Schema(fields=tuple(out_fields))

    if isinstance(node, Explode):
        return s_in.explode(node.columns)

    if isinstance(node, Unnest):
        cols = tuple(it.column for it in node.items)
        return s_in.unnest(cols)[0]

    if isinstance(node, Posexplode):
        value_name = node.column if node.value is None else node.value
        return s_in.posexplode(node.column, pos=node.pos, value=value_name)

    raise PlanFrameSchemaError(
        f"Unsupported plan node for schema propagation: {type(node).__name__}"
    )
