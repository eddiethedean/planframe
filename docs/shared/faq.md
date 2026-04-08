# FAQ

## Is PlanFrame a dataframe library?

No. PlanFrame is a typed planning layer that delegates execution to a backend via an adapter.

## Does chaining execute backend work?

No. PlanFrame is always lazy; execution happens at explicit boundaries like `collect()`.

