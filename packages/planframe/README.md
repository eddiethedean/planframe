## planframe

Core package for PlanFrame (typed planning layer). Import as `planframe`.

### What you get
- `planframe.Frame`: immutable, schema-aware transformation plan (**always lazy**)
- `planframe.expr`: typed expression IR (`col`, `lit`, arithmetic/compare/boolean ops, `coalesce`, `if_else`, etc.)
- `planframe.schema`: schema reflection (dataclass + Pydantic) and materialization

### Note on backends
`planframe` is backend-agnostic. It does not execute anything until `collect()` (even for eager backends). To execute plans you need an adapter package (e.g. `planframe-polars`).

### Typing
PlanFrame includes `py.typed` plus generated stubs (notably `planframe/frame.pyi`) to improve static typing in editors and Pyright.

If you modify the `Frame` API, regenerate stubs from the repo root:

```bash
python scripts/generate_typing_stubs.py
python scripts/generate_typing_stubs.py --check
```
 
