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
API_KEY = "iyree_sk_pYCVsTLC02TF3QWllqUco1ldb60-AZzYreANqJDlR_FwwPELqfHUUQ"

TESTS_DIR = Path(__file__).parent
S3_UPLOAD_DIR = TESTS_DIR / "s3" / "objects_to_upload"
S3_DOWNLOAD_DIR = TESTS_DIR / "s3" / "downloaded_objects"

TABLE = "fact_product_balance"
SUMMARY_TABLE = "fact_product_balance_summary"
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

    All ID columns are VARCHAR(64) in StarRocks, so we produce strings.
    ``date_add`` is DATETIME, ``loaded_at`` is DATE.
    """
    rng = random.Random(42)
    base_date = datetime(2025, 6, 1)
    rows = []
    for i in range(n):
        day_offset = rng.randint(0, 180)
        dt = base_date + timedelta(days=day_offset)
        rows.append({
            "organization_id": str(org_offset + rng.randint(1, 50)),
            "terminal_group_id": str(rng.randint(100, 999)),
            "product_id": str(rng.randint(1000, 9999)),
            "product_size_id": str(rng.randint(1, 20)),
            "date_add": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "loaded_at": dt.strftime("%Y-%m-%d"),
            "balance": round(rng.uniform(0, 1000), 2),
        })
    return pd.DataFrame(rows)


@pytest.fixture(autouse=True)
def _rate_limit_pause():
    """Delay between tests to avoid hitting gateway rate limits."""
    import time
    time.sleep(1)
    yield
    time.sleep(1)


@pytest.fixture(scope="module")
def client():
    with IyreeClient(
        api_key=API_KEY, gateway_host=GATEWAY_HOST,
        timeout=60.0, stream_load_timeout=120.0,
        max_retries=8,
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
# DWH — Raw SQL (streaming /rawSql endpoint)
# ══════════════════════════════════════════════════════════════════════

class TestDwhRawSqlIntegration:
    """Tests for the streaming raw_sql() method that supports all SQL types."""

    # ── SELECT queries ───────────────────────────────────────────────

    def test_select_query(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 5"
        )
        assert len(result.columns) == 2
        assert "organization_id" in result.columns
        assert "balance" in result.columns
        assert len(result.rows) <= 5
        assert result.row_count <= 5
        for row in result.rows:
            assert "organization_id" in row
            assert "balance" in row

    def test_select_one(self, client: IyreeClient):
        result = client.dwh.raw_sql("SELECT 1 AS n, 'hello' AS greeting")
        assert result.columns == ["n", "greeting"]
        assert len(result.rows) == 1
        assert result.rows[0]["n"] == 1
        assert result.rows[0]["greeting"] == "hello"
        assert result.row_count == 1

    def test_select_empty(self, client: IyreeClient):
        result = client.dwh.raw_sql(f"SELECT * FROM {TABLE} WHERE 1 = 0")
        assert len(result.columns) > 0
        assert result.rows == []
        assert result.row_count == 0

    def test_aggregation(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT organization_id, COUNT(*) AS cnt, SUM(balance) AS total "
            f"FROM {TABLE} "
            f"GROUP BY organization_id "
            f"ORDER BY cnt DESC LIMIT 5"
        )
        assert "cnt" in result.columns
        assert "total" in result.columns
        for row in result.rows:
            assert row["cnt"] > 0

    def test_order_by(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT balance FROM {TABLE} "
            f"WHERE balance IS NOT NULL "
            f"ORDER BY balance ASC LIMIT 20"
        )
        balances = [float(row["balance"]) for row in result.rows]
        assert balances == sorted(balances)

    def test_subquery(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT sub.org_id, sub.total "
            f"FROM ("
            f"  SELECT organization_id AS org_id, SUM(balance) AS total "
            f"  FROM {TABLE} GROUP BY organization_id"
            f") sub "
            f"WHERE sub.total > 0 "
            f"ORDER BY sub.total DESC LIMIT 5"
        )
        assert "org_id" in result.columns
        assert "total" in result.columns
        for row in result.rows:
            assert float(row["total"]) > 0

    def test_window_function(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT organization_id, balance, "
            f"  ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY balance DESC) AS rn "
            f"FROM {TABLE} LIMIT 50"
        )
        assert "rn" in result.columns

    def test_case_expression(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT balance, "
            f"  CASE WHEN balance >= 500 THEN 'high' "
            f"       WHEN balance >= 100 THEN 'medium' "
            f"       ELSE 'low' END AS tier "
            f"FROM {TABLE} LIMIT 20"
        )
        assert "tier" in result.columns
        for row in result.rows:
            assert row["tier"] in ("high", "medium", "low")

    # ── Result conversion ────────────────────────────────────────────

    def test_to_dicts(self, client: IyreeClient):
        result = client.dwh.raw_sql(f"SELECT balance FROM {TABLE} LIMIT 3")
        dicts = result.to_dicts()
        assert isinstance(dicts, list)
        if dicts:
            assert "balance" in dicts[0]

    def test_to_dataframe(self, client: IyreeClient):
        result = client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 10"
        )
        df = result.to_dataframe()
        assert list(df.columns) == ["organization_id", "balance"]
        assert len(df) <= 10

    # ── DML: INSERT INTO values ──────────────────────────────────────

    def test_insert_values(self, client: IyreeClient):
        """Insert specific rows into fact_product_balance via raw_sql."""
        result = client.dwh.raw_sql(
            f"INSERT INTO {TABLE} "
            f"(organization_id, terminal_group_id, product_id, "
            f" product_size_id, date_add, loaded_at, balance) "
            f"VALUES "
            f"('sdk-raw-org-1', 'tg-100', 'p-200', 's-1', "
            f" '2025-06-01 00:00:00', '2025-06-01', 123.45), "
            f"('sdk-raw-org-2', 'tg-101', 'p-201', 's-2', "
            f" '2025-06-02 00:00:00', '2025-06-02', 678.90)"
        )
        assert result.affected_rows == 2
        assert result.rows == []

    def test_inserted_values_are_queryable(self, client: IyreeClient):
        """Verify the rows inserted by the previous test are queryable."""
        result = client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {TABLE} "
            f"WHERE organization_id IN ('sdk-raw-org-1', 'sdk-raw-org-2')"
        )
        found_orgs = {row["organization_id"] for row in result.rows}
        assert "sdk-raw-org-1" in found_orgs or "sdk-raw-org-2" in found_orgs

    # ── DML: INSERT INTO ... SELECT (aggregation into summary) ───────

    def test_insert_as_select_into_summary(self, client: IyreeClient):
        """Aggregate fact_product_balance and insert into the summary table."""
        insert_result = client.dwh.raw_sql(
            f"INSERT INTO {SUMMARY_TABLE} (organization_id, balance) "
            f"SELECT organization_id, SUM(balance) AS balance "
            f"FROM {TABLE} "
            f"GROUP BY organization_id"
        )
        assert insert_result.affected_rows > 0

    def test_summary_table_has_data(self, client: IyreeClient):
        """Verify the summary table was populated by the previous INSERT."""
        result = client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {SUMMARY_TABLE} LIMIT 10"
        )
        assert result.row_count > 0
        for row in result.rows:
            assert "organization_id" in row
            assert "balance" in row

    def test_summary_matches_source_aggregation(self, client: IyreeClient):
        """Cross-check a summary row against the source aggregation."""
        summary = client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {SUMMARY_TABLE} LIMIT 1"
        )
        if not summary.rows:
            pytest.skip("Summary table is empty")

        org_id = summary.rows[0]["organization_id"]
        source = client.dwh.raw_sql(
            f"SELECT SUM(balance) AS total FROM {TABLE} "
            f"WHERE organization_id = '{org_id}'"
        )
        assert float(summary.rows[0]["balance"]) == pytest.approx(
            float(source.rows[0]["total"]), rel=1e-4,
        )

    # ── SHOW / metadata ─────────────────────────────────────────────

    def test_show_tables(self, client: IyreeClient):
        result = client.dwh.raw_sql("SHOW TABLES")
        assert len(result.columns) >= 1
        assert result.row_count >= 0

    # ── Large streaming result ───────────────────────────────────────

    def test_large_select(self, client: IyreeClient):
        """Verify streaming works for larger result sets."""
        result = client.dwh.raw_sql(f"SELECT * FROM {TABLE} LIMIT 1000")
        assert len(result.rows) <= 1000
        assert result.row_count == len(result.rows)


# ══════════════════════════════════════════════════════════════════════
# DWH — Stream Load (insert)
# ══════════════════════════════════════════════════════════════════════

class TestDwhInsertIntegration:
    def test_json_insert_single(self, client: IyreeClient):
        label = f"sdk_test_{uuid.uuid4().hex[:12]}"
        data = [{
            "organization_id": "org-2", "terminal_group_id": "tg-200",
            "product_id": "p-300", "product_size_id": "s-400",
            "date_add": "2025-01-02 00:00:00", "loaded_at": "2025-01-02",
            "balance": 50.0,
        }]
        result = client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 1

    def test_json_insert_multiple(self, client: IyreeClient):
        label = f"sdk_test_{uuid.uuid4().hex[:12]}"
        data = [
            {
                "organization_id": f"org-{i}", "terminal_group_id": f"tg-{100 + i}",
                "product_id": f"p-{1000 + i}", "product_size_id": "s-1",
                "date_add": "2025-04-01 00:00:00", "loaded_at": "2025-04-01",
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
            f"WHERE organization_id LIKE '90%'"
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

    # ── List ─────────────────────────────────────────────────────────

    def test_list_basic(self, client: IyreeClient):
        """Insert docs, list them, verify they appear."""
        prefix = uuid.uuid4().hex[:8]
        keys = [f"list-{prefix}-{i}" for i in range(5)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"idx": k}, key=k)

        result = client.kv.list(self.VARIABLE, limit=100)
        found_keys = {doc.key for doc in result.items}
        for k in keys:
            assert k in found_keys, f"{k} not in list result"

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_list_with_limit(self, client: IyreeClient):
        """Verify limit parameter is respected."""
        prefix = uuid.uuid4().hex[:8]
        keys = [f"lim-{prefix}-{i}" for i in range(5)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"x": 1}, key=k)

        result = client.kv.list(self.VARIABLE, limit=2)
        assert len(result.items) <= 2

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_list_pagination_with_cursor(self, client: IyreeClient):
        """Verify cursor-based pagination works."""
        prefix = uuid.uuid4().hex[:8]
        keys = [f"pg-{prefix}-{i}" for i in range(5)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"x": 1}, key=k)

        all_keys: list[str] = []
        cursor = None
        for _ in range(10):
            page = client.kv.list(self.VARIABLE, limit=2, cursor=cursor)
            all_keys.extend(doc.key for doc in page.items)
            if not page.has_more or page.cursor is None:
                break
            cursor = page.cursor

        for k in keys:
            assert k in all_keys

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_list_iter_auto_paginates(self, client: IyreeClient):
        """Verify list_iter walks through all pages."""
        prefix = uuid.uuid4().hex[:8]
        keys = [f"iter-{prefix}-{i}" for i in range(6)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"x": 1}, key=k)

        found = set()
        for doc in client.kv.list_iter(self.VARIABLE, limit=2):
            found.add(doc.key)
        for k in keys:
            assert k in found

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_list_with_order_by(self, client: IyreeClient):
        """Verify order_by returns documents in the requested order."""
        prefix = uuid.uuid4().hex[:8]
        keys = [f"ord-{prefix}-{i}" for i in range(3)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"x": 1}, key=k)

        result_asc = client.kv.list(
            self.VARIABLE,
            order_by={"field": "created_at", "direction": "asc"},
            limit=100,
        )
        result_desc = client.kv.list(
            self.VARIABLE,
            order_by={"field": "created_at", "direction": "desc"},
            limit=100,
        )
        asc_keys = [d.key for d in result_asc.items]
        desc_keys = [d.key for d in result_desc.items]
        assert asc_keys != desc_keys or len(asc_keys) <= 1

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_list_with_select(self, client: IyreeClient):
        """Verify select parameter limits the data keys returned."""
        doc_key = f"sel-{uuid.uuid4().hex[:12]}"
        client.kv.put(
            self.VARIABLE,
            {"name": "Alice", "age": 30, "city": "Berlin"},
            key=doc_key,
        )

        result = client.kv.list(self.VARIABLE, select=["name"])
        target = next((d for d in result.items if d.key == doc_key), None)
        assert target is not None
        assert "name" in target.data
        assert "city" not in target.data

        client.kv.delete(self.VARIABLE, doc_key)

    def test_list_with_index_filter(self, client: IyreeClient):
        """Verify where clause filters by index value."""
        prefix = uuid.uuid4().hex[:8]
        k_a = f"wh-{prefix}-a"
        k_b = f"wh-{prefix}-b"
        client.kv.put(
            self.VARIABLE, {"v": "a"}, key=k_a,
            indexes={"status": "active"},
        )
        client.kv.put(
            self.VARIABLE, {"v": "b"}, key=k_b,
            indexes={"status": "inactive"},
        )

        result = client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "status", "op": "eq", "value": "active"}],
        )
        found_keys = {d.key for d in result.items}
        assert k_a in found_keys
        assert k_b not in found_keys

        client.kv.delete(self.VARIABLE, k_a)
        client.kv.delete(self.VARIABLE, k_b)

    def test_list_with_datetime_index_filter(self, client: IyreeClient):
        """Verify datetime values in where clauses are serialized correctly."""
        prefix = uuid.uuid4().hex[:8]
        dt_old = datetime(2024, 1, 1, 0, 0, 0)
        dt_new = datetime(2025, 6, 1, 0, 0, 0)

        k_old = f"dt-{prefix}-old"
        k_new = f"dt-{prefix}-new"
        client.kv.put(
            self.VARIABLE, {"v": "old"}, key=k_old,
            indexes={"event_at": dt_old},
        )
        client.kv.put(
            self.VARIABLE, {"v": "new"}, key=k_new,
            indexes={"event_at": dt_new},
        )

        result = client.kv.list(
            self.VARIABLE,
            where=[{
                "index_name": "event_at",
                "op": "gte",
                "value": datetime(2025, 1, 1),
            }],
        )
        found_keys = {d.key for d in result.items}
        assert k_new in found_keys
        assert k_old not in found_keys

        client.kv.delete(self.VARIABLE, k_old)
        client.kv.delete(self.VARIABLE, k_new)

    def test_list_empty(self, client: IyreeClient):
        """List with an impossible filter returns empty items."""
        result = client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "nonexistent_idx", "op": "eq", "value": "nope"}],
        )
        assert result.items == []

    # ── Bulk get ─────────────────────────────────────────────────────

    def test_bulk_get_all_found(self, client: IyreeClient):
        prefix = uuid.uuid4().hex[:8]
        keys = [f"bg-{prefix}-{i}" for i in range(3)]
        for k in keys:
            client.kv.put(self.VARIABLE, {"k": k}, key=k)

        result = client.kv.bulk_get(self.VARIABLE, keys)
        assert len(result) == 3
        for k in keys:
            assert k in result
            assert result[k].data["k"] == k

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_bulk_get_partial(self, client: IyreeClient):
        """Missing keys should be absent from the result, not raise."""
        doc_key = f"bg-partial-{uuid.uuid4().hex[:12]}"
        client.kv.put(self.VARIABLE, {"x": 1}, key=doc_key)

        result = client.kv.bulk_get(
            self.VARIABLE,
            [doc_key, "definitely-not-a-key"],
        )
        assert doc_key in result
        assert "definitely-not-a-key" not in result

        client.kv.delete(self.VARIABLE, doc_key)

    def test_bulk_get_empty_keys(self, client: IyreeClient):
        result = client.kv.bulk_get(self.VARIABLE, [])
        assert result == {}

    # ── Bulk put ─────────────────────────────────────────────────────

    def test_bulk_put_basic(self, client: IyreeClient):
        prefix = uuid.uuid4().hex[:8]
        items = [
            {"data": {"name": f"user-{i}"}, "key": f"bp-{prefix}-{i}"}
            for i in range(5)
        ]
        returned_keys = client.kv.bulk_put(self.VARIABLE, items)
        assert len(returned_keys) == 5
        for i, k in enumerate(returned_keys):
            assert k == f"bp-{prefix}-{i}"

        docs = client.kv.bulk_get(self.VARIABLE, returned_keys)
        assert len(docs) == 5
        assert docs[f"bp-{prefix}-0"].data["name"] == "user-0"

        for k in returned_keys:
            client.kv.delete(self.VARIABLE, k)

    def test_bulk_put_auto_generated_keys(self, client: IyreeClient):
        """When key is omitted, the backend generates one."""
        items = [{"data": {"auto": True}} for _ in range(3)]
        returned_keys = client.kv.bulk_put(self.VARIABLE, items)
        assert len(returned_keys) == 3
        assert all(isinstance(k, str) and len(k) > 0 for k in returned_keys)

        for k in returned_keys:
            client.kv.delete(self.VARIABLE, k)

    def test_bulk_put_with_indexes(self, client: IyreeClient):
        """Bulk put with indexes, then filter by index in list."""
        prefix = uuid.uuid4().hex[:8]
        items = [
            {
                "data": {"v": "a"},
                "key": f"bpi-{prefix}-a",
                "indexes": {"tier": "gold"},
            },
            {
                "data": {"v": "b"},
                "key": f"bpi-{prefix}-b",
                "indexes": {"tier": "silver"},
            },
        ]
        keys = client.kv.bulk_put(self.VARIABLE, items)
        assert len(keys) == 2

        result = client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "tier", "op": "eq", "value": "gold"}],
        )
        found_keys = {d.key for d in result.items}
        assert f"bpi-{prefix}-a" in found_keys
        assert f"bpi-{prefix}-b" not in found_keys

        for k in keys:
            client.kv.delete(self.VARIABLE, k)

    def test_bulk_put_with_datetime_indexes(self, client: IyreeClient):
        """Verify datetime index values are serialized correctly in bulk_put."""
        prefix = uuid.uuid4().hex[:8]
        dt = datetime(2025, 7, 15, 10, 30, 0)
        items = [
            {
                "data": {"event": "launch"},
                "key": f"bpdt-{prefix}",
                "indexes": {"happened_at": dt},
            },
        ]
        keys = client.kv.bulk_put(self.VARIABLE, items)
        assert len(keys) == 1

        doc = client.kv.get(self.VARIABLE, keys[0])
        assert doc.data["event"] == "launch"

        client.kv.delete(self.VARIABLE, keys[0])

    def test_bulk_put_upsert(self, client: IyreeClient):
        """Bulk put with upsert overwrites existing docs."""
        doc_key = f"bpu-{uuid.uuid4().hex[:12]}"
        client.kv.put(self.VARIABLE, {"version": 1}, key=doc_key)

        client.kv.bulk_put(self.VARIABLE, [
            {"data": {"version": 2}, "key": doc_key},
        ], upsert=True)

        doc = client.kv.get(self.VARIABLE, doc_key)
        assert doc.data["version"] == 2

        client.kv.delete(self.VARIABLE, doc_key)

    def test_bulk_put_with_ttl(self, client: IyreeClient):
        """Bulk put with TTL creates documents that have expires_at set."""
        doc_key = f"bpttl-{uuid.uuid4().hex[:12]}"
        client.kv.bulk_put(self.VARIABLE, [
            {"data": {"temp": True}, "key": doc_key, "ttl": 3600},
        ])

        doc = client.kv.get(self.VARIABLE, doc_key)
        assert doc.expires_at is not None

        client.kv.delete(self.VARIABLE, doc_key)


# ══════════════════════════════════════════════════════════════════════
# Module-level API (iyree.init / iyree.dwh / iyree.cube / ...)
# ══════════════════════════════════════════════════════════════════════

class TestModuleLevelApiIntegration:
    """Verify the ``iyree.init()`` → ``iyree.dwh.sql()`` pattern works end-to-end."""

    def setup_method(self):
        import iyree
        iyree.init(api_key=API_KEY, gateway_host=GATEWAY_HOST, timeout=60.0, max_retries=5)

    def teardown_method(self):
        import iyree
        iyree.close()

    def test_dwh_sql_via_module(self):
        import iyree
        result = iyree.dwh.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}")
        assert int(result.rows[0][0]) >= 0

    def test_cube_load_via_module(self):
        import iyree
        result = iyree.cube.load({
            "measures": ["dm_order.dish_sum_int"],
            "dimensions": ["dm_order.department_name"],
            "timeDimensions": [{
                "dimension": "dm_order.open_date_typed",
                "dateRange": ["2025-12-01", "2025-12-21"],
            }],
            "filters": [],
        })
        assert isinstance(result.data, list)

    def test_kv_via_module(self):
        import iyree
        doc_key = f"module-api-test-{uuid.uuid4().hex[:12]}"
        iyree.kv.put("myvar", {"module": True}, key=doc_key)
        doc = iyree.kv.get("myvar", doc_key)
        assert doc.data["module"] is True
        iyree.kv.delete("myvar", doc_key)
