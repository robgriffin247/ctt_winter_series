The ingestion layer extracts raw data from sources (zwiftracing.app and zpdatafetch) and loads them into the database as tables of raw data. Some column selection occurs in the ``zrapp`` ingestion as raw data in the zp results source is badly structured and formatted; it leads to a number of nested tables without need, and numeric values can be transmitted from zrapp as both integers and floats leading to variant columns in dlt.

Ingestion is performed with:

```
uv run python3 ingestion/zrapp.py <event_id>
uv run python3 ingestion/zpdf.py <event_id>
uv run python3 ingestion/google_sheets.py
```

