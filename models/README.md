The transformation layer takes distributed raw data and combines and transforms it into production-ready data.

- Staging is to take raw data into staging layer, setting column names, data types and selecting columns.
- Intermediate is the major transformation layer, with joins, decoding, derivations.
- Core is the production-ready layer; selecting columns and sorting rows needed to produce analytics outputs

Transformation is performed with:

```
uv run duckdb build
```

#### To Do

- [ ] Add selection and sorting to core layer
- [ ] Add user-friendly formatting to values like time_seconds 