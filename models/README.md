The transformation layer takes distributed raw data and combines and transforms it into production-ready data.

- Staging is to take raw data and seeds into staging layer, setting column names, data types and selecting columns.
- Intermediate is the major transformation layer, with joins, decoding, derivations.
- Core is the production-ready layer; selecting columns and sorting rows needed to produce analytics outputs

Transformation is performed with:

```
uv run duckdb build
```

#### To Do

- Working on moving logic for leaderboard into the front end
    - dim_round_efforts now redundant? remember best_race
    - Remove mixed and have that handled at front-end (if mixed then filter True)