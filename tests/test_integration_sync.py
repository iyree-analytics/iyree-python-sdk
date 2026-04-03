"""Synchronous integration tests against a real gateway.

Run with:  poetry run pytest tests/test_integration_sync.py -v -m integration
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from iyree import (
    IyreeClient,
    IyreeNotFoundError,
    IyreeError,
)

pytestmark = pytest.mark.integration

GATEWAY_HOST = "http://localhost:9080"
API_KEY = "iyree_sk_ZJTdq9HGV_6sgpBRb0fiFHmj7ht_3R8zJ3WUy4wIZDqfdN47j4PcMQ"

TESTS_DIR = Path(__file__).parent
S3_UPLOAD_DIR = TESTS_DIR / "s3" / "objects_to_upload"
S3_DOWNLOAD_DIR = TESTS_DIR / "s3" / "downloaded_objects"

TABLE = "fact_product_balance"
COLUMNS = [
    "organization_id", "terminal_group_id", "product_id",
    "product_size_id", "date_add", "loaded_at", "balance",
]

has_pyarrow = pytest.importorskip("pyarrow", reason="pyarrow required for parquet tests") if False else None
try:
    import pyarrow  # noqa: F401
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


def generate_fact_rows(n: int, *, org_offset: int = 9000) -> pd.DataFrame:
    """Generate *n* rows compatible with fact_product_balance.

    Integer columns are explicitly typed to avoid float CSV serialization.
    """
    rng = random.Random(42)
    base_date = datetime(2025, 6, 1)
    rows = []
    for i in range(n):
        day_offset = rng.randint(0, 180)
        dt = base_date + timedelta(days=day_offset)
        rows.append({
            "organization_id": org_offset + rng.randint(1, 50),
            "terminal_group_id": rng.randint(100, 999),
            "product_id": rng.randint(1000, 9999),
            "product_size_id": rng.randint(1, 20),
            "date_add": dt.strftime("%Y-%m-%d"),
            "loaded_at": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "balance": round(rng.uniform(0, 1000), 2),
        })
    df = pd.DataFrame(rows)
    for col in ["organization_id", "terminal_group_id", "product_id", "product_size_id"]:
        df[col] = df[col].astype(int)
    return df


@pytest.fixture(scope="module")
def client():
    with IyreeClient(
        api_key=API_KEY, gateway_host=GATEWAY_HOST,
        timeout=60.0, stream_load_timeout=120.0,
        max_retries=5,
    ) as c:
        yield c


@pytest.fixture(scope="module")
def large_df() -> pd.DataFrame:
    """5 000-row dataset for bulk operations."""
    return generate_fact_rows(5_000)


@pytest.fixture(scope="module")
def s3_parquet_key(client: IyreeClient, large_df: pd.DataFrame) -> str:
    """Upload the large dataset as parquet to S3 once for the module."""
    if not HAS_PYARROW:
        pytest.skip("pyarrow is required for parquet tests")
    key = f"sdk-integration-test/bulk_{uuid.uuid4().hex[:8]}.parquet"
    client.s3.upload_dataframe(key, large_df, format="parquet")
    yield key
    client.s3.delete_objects([key])


# ══════════════════════════════════════════════════════════════════════
# DWH — SQL (advanced)
# ══════════════════════════════════════════════════════════════════════

class TestDwhSqlIntegration:
    def test_select_one(self, client: IyreeClient):
        result = client.dwh.sql("SELECT 1 AS n")
        assert len(result.rows) == 1
        assert int(result.rows[0][0]) == 1

    def test_query_fact_table(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, product_id, balance "
            f"FROM {TABLE} LIMIT 5"
        )
        assert len(result.columns) == 3
        assert len(result.rows) <= 5
        assert result.statistics is not None

    def test_to_dicts(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 3"
        )
        dicts = result.to_dicts()
        assert isinstance(dicts, list)
        if dicts:
            assert "organization_id" in dicts[0]
            assert "balance" in dicts[0]

    def test_to_dataframe(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 3"
        )
        df = result.to_dataframe()
        assert list(df.columns) == ["organization_id", "balance"]
        assert len(df) <= 3

    def test_empty_result(self, client: IyreeClient):
        result = client.dwh.sql(f"SELECT * FROM {TABLE} WHERE 1 = 0")
        assert result.rows == []
        assert len(result.columns) > 0

    # ── Aggregation ──────────────────────────────────────────────────

    def test_aggregation_group_by(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, COUNT(*) AS cnt, SUM(balance) AS total "
            f"FROM {TABLE} "
            f"GROUP BY organization_id "
            f"ORDER BY cnt DESC "
            f"LIMIT 10"
        )
        assert len(result.columns) == 3
        col_names = [c.name for c in result.columns]
        assert "organization_id" in col_names
        assert "cnt" in col_names
        assert "total" in col_names
        assert len(result.rows) <= 10
        for row in result.rows:
            assert int(row[1]) > 0

    def test_aggregation_having(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, AVG(balance) AS avg_bal "
            f"FROM {TABLE} "
            f"GROUP BY organization_id "
            f"HAVING AVG(balance) > 0 "
            f"ORDER BY avg_bal DESC "
            f"LIMIT 5"
        )
        assert len(result.rows) <= 5
        for row in result.rows:
            assert float(row[1]) > 0

    # ── Sorting ──────────────────────────────────────────────────────

    def test_order_by_asc(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT balance FROM {TABLE} "
            f"WHERE balance IS NOT NULL "
            f"ORDER BY balance ASC LIMIT 20"
        )
        balances = [float(r[0]) for r in result.rows]
        assert balances == sorted(balances)

    def test_order_by_desc(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT balance FROM {TABLE} "
            f"WHERE balance IS NOT NULL "
            f"ORDER BY balance DESC LIMIT 20"
        )
        balances = [float(r[0]) for r in result.rows]
        assert balances == sorted(balances, reverse=True)

    def test_order_by_multiple_columns(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, product_id, balance "
            f"FROM {TABLE} "
            f"ORDER BY organization_id ASC, balance DESC "
            f"LIMIT 50"
        )
        assert len(result.rows) <= 50

    # ── Subqueries ───────────────────────────────────────────────────

    def test_subquery_in_where(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, balance "
            f"FROM {TABLE} "
            f"WHERE organization_id IN ("
            f"  SELECT organization_id FROM {TABLE} "
            f"  GROUP BY organization_id "
            f"  HAVING COUNT(*) > 1"
            f") "
            f"LIMIT 20"
        )
        assert len(result.rows) <= 20

    def test_subquery_derived_table(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT sub.org_id, sub.total_balance "
            f"FROM ("
            f"  SELECT organization_id AS org_id, SUM(balance) AS total_balance "
            f"  FROM {TABLE} "
            f"  GROUP BY organization_id"
            f") sub "
            f"WHERE sub.total_balance > 0 "
            f"ORDER BY sub.total_balance DESC "
            f"LIMIT 10"
        )
        col_names = [c.name for c in result.columns]
        assert "org_id" in col_names
        assert "total_balance" in col_names

    def test_subquery_scalar(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT COUNT(*) AS cnt "
            f"FROM {TABLE} "
            f"WHERE balance > (SELECT AVG(balance) FROM {TABLE})"
        )
        assert len(result.rows) == 1
        assert int(result.rows[0][0]) >= 0

    # ── Window functions ─────────────────────────────────────────────

    def test_window_function_row_number(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, balance, "
            f"  ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY balance DESC) AS rn "
            f"FROM {TABLE} "
            f"LIMIT 50"
        )
        col_names = [c.name for c in result.columns]
        assert "rn" in col_names
        assert len(result.rows) <= 50

    # ── CASE / expressions ───────────────────────────────────────────

    def test_case_expression(self, client: IyreeClient):
        result = client.dwh.sql(
            f"SELECT organization_id, balance, "
            f"  CASE "
            f"    WHEN balance >= 500 THEN 'high' "
            f"    WHEN balance >= 100 THEN 'medium' "
            f"    ELSE 'low' "
            f"  END AS tier "
            f"FROM {TABLE} LIMIT 20"
        )
        col_names = [c.name for c in result.columns]
        assert "tier" in col_names
        for row in result.rows:
            assert row[2] in ("high", "medium", "low")

    # ── Insert from S3 parquet file ──────────────────────────────────

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow required")
    def test_insert_from_s3_parquet(self, client: IyreeClient, s3_parquet_key: str):
        """Download parquet from S3 and verify it's readable."""
        data = client.s3.download_object(s3_parquet_key)
        import io
        df = pd.read_parquet(io.BytesIO(data))
        assert len(df) == 5_000
        assert set(df.columns) == set(COLUMNS)


# ══════════════════════════════════════════════════════════════════════
# DWH — Stream Load (insert)
# ══════════════════════════════════════════════════════════════════════

class TestDwhInsertIntegration:
    def test_json_insert_single(self, client: IyreeClient):
        label = f"sdk_test_{uuid.uuid4().hex[:12]}"
        data = [{
            "organization_id": 2, "terminal_group_id": 200,
            "product_id": 300, "product_size_id": 400,
            "date_add": "2025-01-02", "loaded_at": "2025-01-02 00:00:00",
            "balance": 50.0,
        }]
        result = client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 1

    def test_json_insert_multiple(self, client: IyreeClient):
        label = f"sdk_test_{uuid.uuid4().hex[:12]}"
        data = [
            {
                "organization_id": i, "terminal_group_id": 100 + i,
                "product_id": 1000 + i, "product_size_id": 1,
                "date_add": "2025-04-01", "loaded_at": "2025-04-01 00:00:00",
                "balance": round(10.5 * i, 2),
            }
            for i in range(5, 10)
        ]
        result = client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 5

    # ── Large bulk inserts ───────────────────────────────────────────

    def test_bulk_json_insert_3000_rows(self, client: IyreeClient):
        """Insert 3 000 rows via JSON Stream Load."""
        df = generate_fact_rows(3_000, org_offset=8000)
        data = df.to_dict(orient="records")
        label = f"sdk_bulk_json_{uuid.uuid4().hex[:12]}"
        result = client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 3_000

    def test_bulk_json_insert_5000_rows(self, client: IyreeClient, large_df: pd.DataFrame):
        """Insert 5 000 rows via JSON Stream Load."""
        data = large_df.to_dict(orient="records")
        label = f"sdk_bulk_json5k_{uuid.uuid4().hex[:12]}"
        result = client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 5_000

    def test_verify_bulk_insert_queryable(self, client: IyreeClient):
        """Verify rows from bulk inserts are queryable."""
        result = client.dwh.sql(
            f"SELECT COUNT(*) AS cnt FROM {TABLE} "
            f"WHERE organization_id BETWEEN 9000 AND 9050"
        )
        assert int(result.rows[0][0]) > 0


# ══════════════════════════════════════════════════════════════════════
# Cube
# ══════════════════════════════════════════════════════════════════════

class TestCubeIntegration:
    def test_load_raw_dict(self, client: IyreeClient):
        query = {
            "measures": [
                "dm_order.dish_sum_int",
                "dm_order.dish_sum_int_less_1m_null",
            ],
            "dimensions": ["dm_order.department_name"],
            "timeDimensions": [{
                "dimension": "dm_order.open_date_typed",
                "dateRange": ["2025-12-01", "2025-12-21"],
            }],
            "filters": [],
        }
        result = client.cube.load(query)
        assert isinstance(result.data, list)
        assert isinstance(result.annotation, dict)
        assert result.raw_response is not None

    def test_load_to_dataframe(self, client: IyreeClient):
        query = {
            "measures": ["dm_order.dish_sum_int"],
            "dimensions": ["dm_order.department_name"],
            "timeDimensions": [{
                "dimension": "dm_order.open_date_typed",
                "dateRange": ["2025-12-01", "2025-12-21"],
            }],
            "filters": [],
        }
        result = client.cube.load(query)
        df = result.to_dataframe()
        assert len(df) >= 0

    def test_load_with_query_builder(self, client: IyreeClient):
        from iyree import Cube, Query, TimeDimension, DateRange

        dm_order = Cube("dm_order")
        query = Query(
            measures=[
                dm_order.measure("dish_sum_int"),
                dm_order.measure("dish_sum_int_less_1m_null"),
            ],
            dimensions=[dm_order.dimension("department_name")],
            time_dimensions=[
                TimeDimension(
                    dm_order.dimension("open_date_typed"),
                    date_range=DateRange(
                        start_date="2025-12-01", end_date="2025-12-21",
                    ),
                )
            ],
        )
        result = client.cube.load(query)
        assert isinstance(result.data, list)

    @pytest.mark.xfail(reason="Cube meta endpoint may not be registered on the gateway")
    def test_meta(self, client: IyreeClient):
        meta = client.cube.meta()
        assert isinstance(meta, dict)


# ══════════════════════════════════════════════════════════════════════
# S3
# ══════════════════════════════════════════════════════════════════════

class TestS3Integration:
    TEST_PREFIX = "sdk-integration-test/"

    # ── Basic operations ─────────────────────────────────────────────

    def test_upload_and_download(self, client: IyreeClient):
        key = f"{self.TEST_PREFIX}{uuid.uuid4().hex}.txt"
        content = b"hello from iyree sdk integration test"

        client.s3.upload_object(key, content, content_type="text/plain")
        downloaded = client.s3.download_object(key)
        assert downloaded == content
        client.s3.delete_objects([key])

    def test_upload_download_to_file(self, client: IyreeClient, tmp_path):
        key = f"{self.TEST_PREFIX}{uuid.uuid4().hex}.txt"
        content = b"file download test"

        client.s3.upload_object(key, content)
        local_path = tmp_path / "downloaded.txt"
        client.s3.download_object_to_file(key, local_path)
        assert local_path.read_bytes() == content
        client.s3.delete_objects([key])

    def test_list_objects(self, client: IyreeClient):
        key = f"{self.TEST_PREFIX}list-test-{uuid.uuid4().hex[:8]}.txt"
        client.s3.upload_object(key, b"list test")

        result = client.s3.list_objects(prefix=self.TEST_PREFIX, max_keys=100)
        keys = [obj.key for obj in result.objects]
        assert key in keys
        client.s3.delete_objects([key])

    def test_list_objects_iter(self, client: IyreeClient):
        key = f"{self.TEST_PREFIX}iter-test-{uuid.uuid4().hex[:8]}.txt"
        client.s3.upload_object(key, b"iter test")

        found = False
        for obj in client.s3.list_objects_iter(prefix=self.TEST_PREFIX):
            if obj.key == key:
                found = True
                break
        assert found
        client.s3.delete_objects([key])

    def test_copy_object(self, client: IyreeClient):
        src = f"{self.TEST_PREFIX}copy-src-{uuid.uuid4().hex[:8]}.txt"
        dst = f"{self.TEST_PREFIX}copy-dst-{uuid.uuid4().hex[:8]}.txt"

        client.s3.upload_object(src, b"copy me")
        result = client.s3.copy_object(src, dst)
        assert result.destination_key == dst

        downloaded = client.s3.download_object(dst)
        assert downloaded == b"copy me"
        client.s3.delete_objects([src, dst])

    def test_delete_objects(self, client: IyreeClient):
        key = f"{self.TEST_PREFIX}delete-{uuid.uuid4().hex[:8]}.txt"
        client.s3.upload_object(key, b"delete me")

        result = client.s3.delete_objects([key])
        assert key in result.deleted

    def test_upload_dataframe_csv(self, client: IyreeClient):
        key = f"{self.TEST_PREFIX}df-{uuid.uuid4().hex[:8]}.csv"
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        client.s3.upload_dataframe(key, df, format="csv")
        downloaded = client.s3.download_object(key)
        assert b"a,b" in downloaded
        client.s3.delete_objects([key])

    # ── Upload local files from tests/s3/objects_to_upload ───────────

    def test_upload_local_files(self, client: IyreeClient):
        """Upload every file from tests/s3/objects_to_upload and verify each."""
        if not S3_UPLOAD_DIR.exists():
            pytest.skip("tests/s3/objects_to_upload directory not found")

        uploaded_keys: list[str] = []
        local_files = sorted(S3_UPLOAD_DIR.iterdir())
        assert len(local_files) > 0, "No files to upload"

        for local_file in local_files:
            if not local_file.is_file():
                continue
            key = f"{self.TEST_PREFIX}uploaded/{local_file.name}"
            content = local_file.read_bytes()
            client.s3.upload_object(key, content)
            uploaded_keys.append(key)

        listed = client.s3.list_objects(
            prefix=f"{self.TEST_PREFIX}uploaded/", max_keys=100,
        )
        listed_keys = {obj.key for obj in listed.objects}
        for key in uploaded_keys:
            assert key in listed_keys, f"{key} not found after upload"

        client.s3.delete_objects(uploaded_keys)

    # ── Download files to tests/s3/downloaded_objects ─────────────────

    def test_download_to_local_dir(self, client: IyreeClient):
        """Upload a few files, then download them to tests/s3/downloaded_objects."""
        S3_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        test_files = {
            "report.txt": b"Quarterly revenue report 2025-Q1",
            "data.csv": b"id,name,value\n1,alpha,100\n2,beta,200\n",
            "config.json": b'{"version": 1, "debug": false}',
        }
        keys: list[str] = []
        for name, content in test_files.items():
            key = f"{self.TEST_PREFIX}to-download/{name}"
            client.s3.upload_object(key, content)
            keys.append(key)

        for name, expected in test_files.items():
            key = f"{self.TEST_PREFIX}to-download/{name}"
            local_path = S3_DOWNLOAD_DIR / name
            client.s3.download_object_to_file(key, local_path)
            assert local_path.exists()
            assert local_path.read_bytes() == expected

        client.s3.delete_objects(keys)
        for f in S3_DOWNLOAD_DIR.iterdir():
            f.unlink()

    # ── Parquet upload/download ──────────────────────────────────────

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow required for parquet")
    def test_upload_large_parquet(self, client: IyreeClient, s3_parquet_key: str):
        """Verify the large parquet file is downloadable and has correct size."""
        import io
        data = client.s3.download_object(s3_parquet_key)
        assert len(data) > 10_000

        df_roundtrip = pd.read_parquet(io.BytesIO(data))
        assert len(df_roundtrip) == 5_000
        assert set(df_roundtrip.columns) == set(COLUMNS)

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow required for parquet")
    def test_upload_dataframe_parquet(self, client: IyreeClient):
        """Upload a DataFrame as parquet and verify round-trip."""
        import io
        key = f"{self.TEST_PREFIX}df-pq-{uuid.uuid4().hex[:8]}.parquet"
        df = generate_fact_rows(500, org_offset=7000)

        client.s3.upload_dataframe(key, df, format="parquet")
        data = client.s3.download_object(key)

        df_back = pd.read_parquet(io.BytesIO(data))
        assert len(df_back) == 500
        client.s3.delete_objects([key])


# ══════════════════════════════════════════════════════════════════════
# KV
# ══════════════════════════════════════════════════════════════════════

class TestKvIntegration:
    VARIABLE = "myvar"

    def test_put_get_delete_lifecycle(self, client: IyreeClient):
        doc_key = f"sdk-test-{uuid.uuid4().hex[:12]}"
        data = {"greeting": "hello", "count": 42}

        returned_key = client.kv.put(self.VARIABLE, data, key=doc_key)
        assert returned_key == doc_key

        doc = client.kv.get(self.VARIABLE, doc_key)
        assert doc.key == doc_key
        assert doc.data["greeting"] == "hello"
        assert doc.data["count"] == 42
        assert doc.created_at is not None

        deleted = client.kv.delete(self.VARIABLE, doc_key)
        assert deleted is True

    def test_exists(self, client: IyreeClient):
        doc_key = f"sdk-test-exists-{uuid.uuid4().hex[:12]}"
        client.kv.put(self.VARIABLE, {"x": 1}, key=doc_key)
        assert client.kv.exists(self.VARIABLE, doc_key) is True
        assert client.kv.exists(self.VARIABLE, "nonexistent-key-12345") is False
        client.kv.delete(self.VARIABLE, doc_key)

    def test_get_not_found(self, client: IyreeClient):
        with pytest.raises(IyreeNotFoundError):
            client.kv.get(self.VARIABLE, "this-key-does-not-exist")

    def test_patch(self, client: IyreeClient):
        doc_key = f"sdk-test-patch-{uuid.uuid4().hex[:12]}"
        client.kv.put(self.VARIABLE, {"name": "Alice", "score": 10}, key=doc_key)

        updated = client.kv.patch(
            self.VARIABLE, doc_key, set={"name": "Bob"}, inc={"score": 5},
        )
        assert updated.data["name"] == "Bob"
        assert updated.data["score"] == 15
        client.kv.delete(self.VARIABLE, doc_key)

    def test_upsert(self, client: IyreeClient):
        doc_key = f"sdk-test-upsert-{uuid.uuid4().hex[:12]}"
        client.kv.put(self.VARIABLE, {"v": 1}, key=doc_key)
        client.kv.put(self.VARIABLE, {"v": 2}, key=doc_key, upsert=True)

        doc = client.kv.get(self.VARIABLE, doc_key)
        assert doc.data["v"] == 2
        client.kv.delete(self.VARIABLE, doc_key)
