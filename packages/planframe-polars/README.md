 ## planframe-polars
 
 Polars adapter package for PlanFrame. Import as `planframe_polars`.

### Usage

```python
import polars as pl
from dataclasses import dataclass

from planframe_polars import from_polars


@dataclass(frozen=True)
class UserSchema:
    id: int
    age: int


lf = pl.DataFrame({"id": [1], "age": [2]}).lazy()
pf = from_polars(lf, schema=UserSchema)
df = pf.select("id").collect()
```

### Execution model

PlanFrame is always lazy:
- Chaining methods (like `.select(...)`) does **not** run Polars operations.
- `collect()` evaluates the full plan. If the source is a `polars.LazyFrame`, this naturally compiles into a single lazy query before collecting.
 
