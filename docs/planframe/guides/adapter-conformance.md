# Adapter conformance kit (third-party `BaseAdapter`)

PlanFrame ships a **small, stable** conformance API so adapter authors can get a **pass/fail** signal in CI without copying PlanFrame’s full backend test matrix.

This is a **development aid**, not a “certified for production” claim.

## Install

From PyPI (when using the published `planframe` core package):

```bash
pip install "planframe[adapter-dev]"
```

The `adapter-dev` extra pulls in **pytest** (and any other lightweight test helpers we add for adapter authors). The conformance runner itself lives in the core package: `planframe.adapter_conformance`.

## What it checks (MVP)

`run_minimal_adapter_conformance` exercises a short list of **named cases** against *your* `Frame` subclass (the same pattern as `planframe_polars.PolarsFrame`):

| Case | Roughly |
|------|--------|
| `select_filter` | `select` + `filter` |
| `project_expr` | `select` with a computed column (`Expr`) |
| `sort` | `sort` by a column |
| `group_by_agg` | `group_by` + tuple reduction agg |
| `join_inner` | `join` inner on a key (optional; see below) |

If something fails, the raised `AssertionError` (or returned `ConformanceResult`) includes the **case name** so failures are actionable.

## API

```python
from planframe.adapter_conformance import run_minimal_adapter_conformance

# Your Frame subclasses, e.g. MyFrame({"id": [...], ...})
run_minimal_adapter_conformance(
    users=MyUsersFrame,
    join_left=MyJoinLeftFrame,
    join_right=MyJoinRightFrame,
)
```

- **`users`**: required. Builds a frame whose rows have **`id` (int), `name` (str), `age` (int)** (same idea as the examples throughout PlanFrame docs).
- **`join_left` / `join_right`**: optional. If either is omitted, the `join_inner` case is **skipped** (reported in `ConformanceResult`, not treated as a failure).

You can pass a **class** with a dict-of-columns constructor, or a **callable** `Mapping[str, Sequence] -> Frame`.

To inspect results without raising:

```python
result = run_minimal_adapter_conformance(users=MyUsersFrame, raise_on_failure=False)
assert result.passed
```

## pytest example (single test)

```python
# tests/test_planframe_adapter_contract.py
from planframe.adapter_conformance import run_minimal_adapter_conformance
from mypkg.frames import Users, JoinLeft, JoinRight

def test_planframe_adapter_contract() -> None:
    run_minimal_adapter_conformance(users=Users, join_left=JoinLeft, join_right=JoinRight)
```

## GitHub Actions recipe

Minimal workflow step after installing your package and test deps:

```yaml
- name: PlanFrame adapter conformance
  run: pytest -q tests/test_planframe_adapter_contract.py
```

Use the same interpreter you use for the rest of your tests (the kit only needs `planframe` + your adapter package).

## Relationship to PlanFrame’s own tests

PlanFrame’s repository also uses a `conformance` **pytest marker** for its full backend matrices (Polars, pandas, etc.). Third-party adapters are expected to depend on this **small** `planframe.adapter_conformance` surface rather than vendoring PlanFrame’s internal test suite.
