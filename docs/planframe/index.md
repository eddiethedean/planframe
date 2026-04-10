# PlanFrame (core)

This track is for **adapter authors**.

PlanFrame core builds a typed plan and delegates execution to a backend via `BaseAdapter`.

## Next steps

- Read the guide: [Creating an adapter](guides/creating-an-adapter.md)
- **Third-party adapters:** run the minimal CI suite — [Adapter conformance kit](guides/adapter-conformance.md)
- Learn the row-export APIs: [Streaming rows](guides/streaming-rows.md)
- Wrapping `Frame` in a host type: [Embedding `Frame` (composition)](guides/embedding-frame.md)
- Optional **API skins** (typed mixins on `Frame`, no extra backend deps): [PySpark-like API](guides/pyspark-like-api.md), [pandas-like API](guides/pandas-like-api.md)
- Browse the design docs under **Design** in the nav (including [Core layout](design/core-layout.md) for how `Frame`, compilation, and `execute_plan` fit together)
- See adapters catalog: [Adapters](../adapters/index.md)

