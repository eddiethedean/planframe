from __future__ import annotations

"""Versioning for PlanFrame's plan and expression IR.

PlanFrame adapters compile expression IR (`planframe.expr`) and execute plan nodes
(`planframe.plan.nodes`). Third-party adapters can use these version markers to
fail predictably when a PlanFrame upgrade introduces new IR shapes they don't yet
support.

These versions are:
- **coarse-grained** (single integer per IR family)
- **monotonic** (only ever increase)
- intended to be used as a compatibility guard, not as a detailed feature matrix.
"""

# Plan IR version.
#
# Bump when a PlanNode shape changes in a way that a downstream adapter (or external
# tooling) would need to update to interpret/execute it correctly.
#
# Examples:
# - a new PlanNode is added (downstream adapter that pattern-matches nodes may need updating)
# - fields on an existing PlanNode are added/removed/renamed or semantics change
PLAN_IR_VERSION: int = 1

# Expression IR version.
#
# Bump when expression node shapes change in a way that affects adapter compilation
# (e.g. a new Expr node is added, AggExpr changes shape, etc.).
EXPR_IR_VERSION: int = 1
