## planframe

Core package for PlanFrame (typed planning layer). Import as `planframe`.

### What you get
- `planframe.Frame`: immutable, schema-aware transformation plan (**always lazy**)
- `planframe.expr`: typed expression IR (`col`, `lit`, `add`, `eq`, boolean ops)
- `planframe.schema`: schema reflection (dataclass + Pydantic) and materialization

### Note on backends
`planframe` is backend-agnostic. It does not execute anything until `collect()` (even for eager backends). To execute plans you need an adapter package (e.g. `planframe-polars`).
 
