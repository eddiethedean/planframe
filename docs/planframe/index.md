# PlanFrame (core)

This track is for **adapter authors**.

PlanFrame core builds a typed plan and delegates execution to a backend via `BaseAdapter`.

## Next steps

- Upgrading from **v1.1.0** or earlier: [Migrating since v1.1.0](guides/migrating-since-1-1.md) (v1.2.0 fixes and additions, including async API, `planframe.materialize`, and Expr operators)
- Read the guide: [Creating an adapter](guides/creating-an-adapter.md)
- **Third-party adapters:** run the minimal CI suite — [Adapter conformance kit](guides/adapter-conformance.md)
- Learn the row-export APIs: [Streaming rows](guides/streaming-rows.md)
- Wrapping `Frame` in a host type: [Embedding `Frame` (composition)](guides/embedding-frame.md)
- Optional **API skins** (typed mixins on `Frame`, no extra backend deps): [PySpark-like API](guides/pyspark-like-api.md), [pandas-like API](guides/pandas-like-api.md)
- Browse the design docs under **Design** in the nav (including [Core layout](design/core-layout.md) for how `Frame`, compilation, and `execute_plan` fit together)
- See adapters catalog: [Adapters](../adapters/index.md)

