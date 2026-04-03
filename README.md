# iyree

Python SDK for the [IYREE](https://iyree.io) BI analytics platform.

```bash
pip install iyree

# With pandas support
pip install iyree[pandas]
```

## Quick Start

### Synchronous

```python
from iyree import IyreeClient

with IyreeClient(api_key="my-key", gateway_host="https://api.iyree.io") as client:
    # DWH — run SQL
    result = client.dwh.sql("SELECT * FROM orders LIMIT 10")
    for row in result.to_dicts():
        print(row)

    # DWH — insert data
    client.dwh.insert("staging_table", "col1,col2\n1,hello\n2,world")

    # Cube — analytics query
    result = client.cube.load({"measures": ["orders.count"]})
    print(result.data)

    # S3 — upload / download
    client.s3.upload_object("data/report.csv", b"a,b\n1,2")
    data = client.s3.download_object("data/report.csv")

    # KV — key-value store
    client.kv.put("config", {"theme": "dark"}, key="user_prefs")
    doc = client.kv.get("config", "user_prefs")
```

### Asynchronous

```python
from iyree import AsyncIyreeClient

async with AsyncIyreeClient(api_key="my-key") as client:
    result = await client.dwh.sql("SELECT 1 AS n")
    print(result.to_dicts())
```

## Configuration

| Parameter | Env Var | Default | Description |
|---|---|---|---|
| `api_key` | — | *required* | API key for gateway auth |
| `gateway_host` | `IYREE_GATEWAY_HOST` | — | Gateway base URL |
| `timeout` | — | `30.0` | Default request timeout (seconds) |
| `stream_load_timeout` | — | `300.0` | DWH Stream Load timeout |
| `cube_continue_wait_timeout` | — | `120.0` | Cube polling timeout |
| `max_retries` | — | `3` | Max retry attempts |

## Cube Query Builder

Build type-safe Cube.js queries:

```python
from iyree import Cube, Query, TimeDimension, DateRange, TimeGranularity

orders = Cube("orders")

query = Query(
    measures=[orders.measure("count"), orders.measure("total_amount")],
    dimensions=[orders.dimension("status")],
    time_dimensions=[
        TimeDimension(
            orders.dimension("created_at"),
            date_range=DateRange(relative="last 30 days"),
            granularity=TimeGranularity.day,
        )
    ],
)

with IyreeClient(api_key="my-key") as client:
    result = client.cube.load(query)
    df = result.to_dataframe()  # requires iyree[pandas]
```

Or use raw dicts:

```python
result = client.cube.load({
    "measures": ["orders.count"],
    "timeDimensions": [{
        "dimension": "orders.created_at",
        "granularity": "day",
        "dateRange": "last 7 days",
    }],
})
```

## DWH — Stream Load (Insert)

```python
import pandas as pd

df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
result = client.dwh.insert("users", df, label="load_20240101")
print(f"Loaded {result.number_loaded_rows} rows")
```

## S3 — Object Storage

```python
# Auto-paginating iterator
for obj in client.s3.list_objects_iter(prefix="data/"):
    print(obj.key, obj.size)

# Stream download to file
client.s3.download_object_to_file("data/large.csv", "/tmp/large.csv")

# Upload a DataFrame as Parquet
client.s3.upload_dataframe("reports/q1.parquet", df, format="parquet")
```

## Error Handling

```python
from iyree import IyreeClient, IyreeAuthError, IyreeNotFoundError

try:
    with IyreeClient(api_key="my-key") as client:
        doc = client.kv.get("store", "missing-key")
except IyreeAuthError:
    print("Invalid API key")
except IyreeNotFoundError:
    print("Document not found")
```

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT
