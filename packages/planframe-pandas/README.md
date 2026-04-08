## planframe-pandas

[![Docs](https://readthedocs.org/projects/planframe/badge/?version=latest)](https://planframe.readthedocs.io/en/latest/planframe_pandas/)
[![PyPI](https://img.shields.io/pypi/v/planframe-pandas)](https://pypi.org/project/planframe-pandas/)
[![License: MIT](https://img.shields.io/badge/License-MIT-informational)](../../LICENSE)

pandas adapter package for PlanFrame. Import as `planframe_pandas`.

Documentation (ReadTheDocs):

- pandas track (end users): `https://planframe.readthedocs.io/en/latest/planframe_pandas/`
- Light API reference: `https://planframe.readthedocs.io/en/latest/planframe_pandas/reference/api/`

### Notes

- PlanFrame is **always lazy**: chaining does not touch backend data; execution happens at `collect()` boundaries.

