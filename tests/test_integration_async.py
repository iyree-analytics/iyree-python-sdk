"""Asynchronous integration tests against a real gateway.

Run with:  poetry run pytest tests/test_integration_async.py -v -m integration
"""

from __future__ import annotations

import io
import uuid

import pandas as pd
import pytest

from iyree import (
    AsyncIyreeClient,
    IyreeNotFoundError,
)
from test_integration_sync import (
    GATEWAY_HOST,
    API_KEY,
    TABLE,
    SUMMARY_TABLE,
    COLUMNS,
    HAS_PYARROW,
    generate_fact_rows,
)

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def _rate_limit_pause():
    """Delay between tests to avoid hitting gateway rate limits."""
    import asyncio
    await asyncio.sleep(1)
    yield
    await asyncio.sleep(1)


@pytest.fixture
async def client():
    c = AsyncIyreeClient(
        api_key=API_KEY, gateway_host=GATEWAY_HOST,
        timeout=60.0, stream_load_timeout=120.0,
        max_retries=8,
    )
    yield c
    await c.close()


# ══════════════════════════════════════════════════════════════════════
# DWH — SQL (advanced)
# ══════════════════════════════════════════════════════════════════════

class TestAsyncDwhSqlIntegration:
    async def test_select_one(self, client: AsyncIyreeClient):
        result = await client.dwh.sql("SELECT 1 AS n")
        assert int(result.rows[0][0]) == 1

    async def test_query_fact_table(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT organization_id, product_id, balance "
            f"FROM {TABLE} LIMIT 5"
        )
        assert len(result.columns) == 3
        assert len(result.rows) <= 5

    async def test_to_dicts(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 3"
        )
        dicts = result.to_dicts()
        assert isinstance(dicts, list)
        if dicts:
            assert "organization_id" in dicts[0]

    async def test_aggregation_group_by(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT organization_id, COUNT(*) AS cnt, SUM(balance) AS total "
            f"FROM {TABLE} "
            f"GROUP BY organization_id "
            f"ORDER BY cnt DESC LIMIT 10"
        )
        assert len(result.columns) == 3
        for row in result.rows:
            assert int(row[1]) > 0

    async def test_subquery_derived_table(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT sub.org_id, sub.total_balance "
            f"FROM ("
            f"  SELECT organization_id AS org_id, SUM(balance) AS total_balance "
            f"  FROM {TABLE} GROUP BY organization_id"
            f") sub "
            f"WHERE sub.total_balance > 0 "
            f"ORDER BY sub.total_balance DESC LIMIT 10"
        )
        col_names = [c.name for c in result.columns]
        assert "org_id" in col_names

    async def test_order_by_asc(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT balance FROM {TABLE} "
            f"WHERE balance IS NOT NULL "
            f"ORDER BY balance ASC LIMIT 20"
        )
        balances = [float(r[0]) for r in result.rows]
        assert balances == sorted(balances)

    async def test_window_function(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT organization_id, balance, "
            f"  ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY balance DESC) AS rn "
            f"FROM {TABLE} LIMIT 50"
        )
        col_names = [c.name for c in result.columns]
        assert "rn" in col_names

    async def test_case_expression(self, client: AsyncIyreeClient):
        result = await client.dwh.sql(
            f"SELECT balance, "
            f"  CASE WHEN balance >= 500 THEN 'high' "
            f"       WHEN balance >= 100 THEN 'medium' "
            f"       ELSE 'low' END AS tier "
            f"FROM {TABLE} LIMIT 10"
        )
        for row in result.rows:
            assert row[1] in ("high", "medium", "low")


# ══════════════════════════════════════════════════════════════════════
# DWH — Raw SQL (streaming /rawSql endpoint)
# ══════════════════════════════════════════════════════════════════════

class TestAsyncDwhRawSqlIntegration:
    async def test_select_query(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 5"
        )
        assert "organization_id" in result.columns
        assert len(result.rows) <= 5
        for row in result.rows:
            assert "organization_id" in row

    async def test_select_one(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql("SELECT 1 AS n")
        assert result.rows[0]["n"] == 1
        assert result.row_count == 1

    async def test_aggregation(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"SELECT organization_id, COUNT(*) AS cnt "
            f"FROM {TABLE} GROUP BY organization_id "
            f"ORDER BY cnt DESC LIMIT 5"
        )
        assert "cnt" in result.columns
        for row in result.rows:
            assert row["cnt"] > 0

    async def test_insert_values(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"INSERT INTO {TABLE} "
            f"(organization_id, terminal_group_id, product_id, "
            f" product_size_id, date_add, loaded_at, balance) "
            f"VALUES "
            f"('async-raw-org-1', 'tg-200', 'p-300', 's-3', "
            f" '2025-06-10 00:00:00', '2025-06-10', 55.55)"
        )
        assert result.affected_rows == 1

    async def test_insert_as_select_into_summary(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"INSERT INTO {SUMMARY_TABLE} (organization_id, balance) "
            f"SELECT organization_id, SUM(balance) AS balance "
            f"FROM {TABLE} GROUP BY organization_id"
        )
        assert result.affected_rows > 0

    async def test_summary_table_queryable(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {SUMMARY_TABLE} LIMIT 5"
        )
        assert result.row_count > 0

    async def test_to_dataframe(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql(
            f"SELECT organization_id, balance FROM {TABLE} LIMIT 5"
        )
        df = result.to_dataframe()
        assert len(df) <= 5
        assert "organization_id" in df.columns

    async def test_show_tables(self, client: AsyncIyreeClient):
        result = await client.dwh.raw_sql("SHOW TABLES")
        assert len(result.columns) >= 1


# ══════════════════════════════════════════════════════════════════════
# DWH — Stream Load (insert)
# ══════════════════════════════════════════════════════════════════════

class TestAsyncDwhInsertIntegration:
    async def test_json_insert(self, client: AsyncIyreeClient):
        label = f"sdk_async_json_{uuid.uuid4().hex[:12]}"
        data = [
            {
                "organization_id": "async-org-8888", "terminal_group_id": "tg-111",
                "product_id": "p-222", "product_size_id": "s-3",
                "date_add": "2025-03-15 00:00:00", "loaded_at": "2025-03-15",
                "balance": 42.5,
            },
            {
                "organization_id": "async-org-8889", "terminal_group_id": "tg-112",
                "product_id": "p-223", "product_size_id": "s-4",
                "date_add": "2025-03-16 00:00:00", "loaded_at": "2025-03-16",
                "balance": 99.9,
            },
        ]
        result = await client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 2

    async def test_bulk_insert_3000_rows(self, client: AsyncIyreeClient):
        df = generate_fact_rows(3_000, org_offset=8500)
        data = df.to_dict(orient="records")
        label = f"sdk_async_bulk_{uuid.uuid4().hex[:12]}"
        result = await client.dwh.insert(TABLE, data, label=label)
        assert result.status == "Success", f"Stream Load failed: {result.message}"
        assert result.number_loaded_rows == 3_000


# ══════════════════════════════════════════════════════════════════════
# Cube
# ══════════════════════════════════════════════════════════════════════

class TestAsyncCubeIntegration:
    async def test_load_raw_dict(self, client: AsyncIyreeClient):
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
        result = await client.cube.load(query)
        assert isinstance(result.data, list)

    async def test_load_with_query_builder(self, client: AsyncIyreeClient):
        from iyree import Cube, Query, TimeDimension, DateRange

        dm_order = Cube("dm_order")
        query = Query(
            measures=[dm_order.measure("dish_sum_int")],
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
        result = await client.cube.load(query)
        assert isinstance(result.data, list)

    @pytest.mark.xfail(reason="Cube meta endpoint may not be registered on the gateway")
    async def test_meta(self, client: AsyncIyreeClient):
        meta = await client.cube.meta()
        assert isinstance(meta, dict)


# ══════════════════════════════════════════════════════════════════════
# S3
# ══════════════════════════════════════════════════════════════════════

class TestAsyncS3Integration:
    TEST_PREFIX = "sdk-async-integration-test/"

    async def test_upload_and_download(self, client: AsyncIyreeClient):
        key = f"{self.TEST_PREFIX}{uuid.uuid4().hex}.txt"
        content = b"async hello from iyree sdk"

        await client.s3.upload_object(key, content, content_type="text/plain")
        downloaded = await client.s3.download_object(key)
        assert downloaded == content
        await client.s3.delete_objects([key])

    async def test_list_objects(self, client: AsyncIyreeClient):
        key = f"{self.TEST_PREFIX}list-{uuid.uuid4().hex[:8]}.txt"
        await client.s3.upload_object(key, b"async list test")

        result = await client.s3.list_objects(prefix=self.TEST_PREFIX, max_keys=100)
        keys = [obj.key for obj in result.objects]
        assert key in keys
        await client.s3.delete_objects([key])

    async def test_list_objects_iter(self, client: AsyncIyreeClient):
        key = f"{self.TEST_PREFIX}iter-{uuid.uuid4().hex[:8]}.txt"
        await client.s3.upload_object(key, b"async iter test")

        found = False
        async for obj in client.s3.list_objects_iter(prefix=self.TEST_PREFIX):
            if obj.key == key:
                found = True
                break
        assert found
        await client.s3.delete_objects([key])

    async def test_copy_and_delete(self, client: AsyncIyreeClient):
        src = f"{self.TEST_PREFIX}cp-src-{uuid.uuid4().hex[:8]}.txt"
        dst = f"{self.TEST_PREFIX}cp-dst-{uuid.uuid4().hex[:8]}.txt"

        await client.s3.upload_object(src, b"copy me async")
        result = await client.s3.copy_object(src, dst)
        assert result.destination_key == dst

        downloaded = await client.s3.download_object(dst)
        assert downloaded == b"copy me async"
        await client.s3.delete_objects([src, dst])

    @pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow required for parquet")
    async def test_upload_large_parquet(self, client: AsyncIyreeClient):
        """Upload 2 000-row parquet and verify round-trip."""
        key = f"{self.TEST_PREFIX}bulk-pq-{uuid.uuid4().hex[:8]}.parquet"
        df = generate_fact_rows(2_000, org_offset=7500)

        await client.s3.upload_dataframe(key, df, format="parquet")
        data = await client.s3.download_object(key)

        df_back = pd.read_parquet(io.BytesIO(data))
        assert len(df_back) == 2_000
        await client.s3.delete_objects([key])


# ══════════════════════════════════════════════════════════════════════
# KV
# ══════════════════════════════════════════════════════════════════════

class TestAsyncKvIntegration:
    VARIABLE = "myvar"

    async def test_put_get_delete_lifecycle(self, client: AsyncIyreeClient):
        doc_key = f"async-sdk-test-{uuid.uuid4().hex[:12]}"
        data = {"greeting": "hello async", "count": 7}

        returned_key = await client.kv.put(self.VARIABLE, data, key=doc_key)
        assert returned_key == doc_key

        doc = await client.kv.get(self.VARIABLE, doc_key)
        assert doc.key == doc_key
        assert doc.data["greeting"] == "hello async"

        deleted = await client.kv.delete(self.VARIABLE, doc_key)
        assert deleted is True

    async def test_exists(self, client: AsyncIyreeClient):
        doc_key = f"async-exists-{uuid.uuid4().hex[:12]}"
        await client.kv.put(self.VARIABLE, {"x": 1}, key=doc_key)
        assert await client.kv.exists(self.VARIABLE, doc_key) is True
        assert await client.kv.exists(self.VARIABLE, "nonexistent-key") is False
        await client.kv.delete(self.VARIABLE, doc_key)

    async def test_get_not_found(self, client: AsyncIyreeClient):
        with pytest.raises(IyreeNotFoundError):
            await client.kv.get(self.VARIABLE, "this-key-does-not-exist")

    async def test_patch(self, client: AsyncIyreeClient):
        doc_key = f"async-patch-{uuid.uuid4().hex[:12]}"
        await client.kv.put(
            self.VARIABLE, {"name": "Alice", "score": 10}, key=doc_key,
        )

        updated = await client.kv.patch(
            self.VARIABLE, doc_key, set={"name": "Bob"}, inc={"score": 5},
        )
        assert updated.data["name"] == "Bob"
        assert updated.data["score"] == 15
        await client.kv.delete(self.VARIABLE, doc_key)

    # ── List ─────────────────────────────────────────────────────────

    async def test_list_basic(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        keys = [f"al-{prefix}-{i}" for i in range(3)]
        for k in keys:
            await client.kv.put(self.VARIABLE, {"i": k}, key=k)

        result = await client.kv.list(self.VARIABLE, limit=100)
        found = {d.key for d in result.items}
        for k in keys:
            assert k in found

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)

    async def test_list_iter(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        keys = [f"ali-{prefix}-{i}" for i in range(4)]
        for k in keys:
            await client.kv.put(self.VARIABLE, {"x": 1}, key=k)

        found = set()
        async for doc in client.kv.list_iter(self.VARIABLE, limit=2):
            found.add(doc.key)
        for k in keys:
            assert k in found

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)

    async def test_list_with_select(self, client: AsyncIyreeClient):
        doc_key = f"asel-{uuid.uuid4().hex[:12]}"
        await client.kv.put(
            self.VARIABLE,
            {"name": "Alice", "age": 30, "city": "Berlin"},
            key=doc_key,
        )
        result = await client.kv.list(self.VARIABLE, select=["name"])
        target = next((d for d in result.items if d.key == doc_key), None)
        assert target is not None
        assert "name" in target.data
        assert "city" not in target.data
        await client.kv.delete(self.VARIABLE, doc_key)

    async def test_list_with_index_filter(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        k_a = f"awh-{prefix}-a"
        k_b = f"awh-{prefix}-b"
        await client.kv.put(
            self.VARIABLE, {"v": "a"}, key=k_a,
            indexes={"color": "red"},
        )
        await client.kv.put(
            self.VARIABLE, {"v": "b"}, key=k_b,
            indexes={"color": "blue"},
        )

        result = await client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "color", "op": "eq", "value": "red"}],
        )
        found = {d.key for d in result.items}
        assert k_a in found
        assert k_b not in found

        await client.kv.delete(self.VARIABLE, k_a)
        await client.kv.delete(self.VARIABLE, k_b)

    async def test_list_with_datetime_index_filter(self, client: AsyncIyreeClient):
        from datetime import datetime as dt
        prefix = uuid.uuid4().hex[:8]
        k1 = f"adt-{prefix}-1"
        k2 = f"adt-{prefix}-2"
        await client.kv.put(
            self.VARIABLE, {"v": 1}, key=k1,
            indexes={"ts": dt(2024, 1, 1)},
        )
        await client.kv.put(
            self.VARIABLE, {"v": 2}, key=k2,
            indexes={"ts": dt(2025, 6, 1)},
        )

        result = await client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "ts", "op": "gte", "value": dt(2025, 1, 1)}],
        )
        found = {d.key for d in result.items}
        assert k2 in found
        assert k1 not in found

        await client.kv.delete(self.VARIABLE, k1)
        await client.kv.delete(self.VARIABLE, k2)

    # ── Bulk get ─────────────────────────────────────────────────────

    async def test_bulk_get(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        keys = [f"abg-{prefix}-{i}" for i in range(3)]
        for k in keys:
            await client.kv.put(self.VARIABLE, {"k": k}, key=k)

        result = await client.kv.bulk_get(self.VARIABLE, keys)
        assert len(result) == 3
        for k in keys:
            assert k in result

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)

    async def test_bulk_get_partial(self, client: AsyncIyreeClient):
        doc_key = f"abgp-{uuid.uuid4().hex[:12]}"
        await client.kv.put(self.VARIABLE, {"x": 1}, key=doc_key)

        result = await client.kv.bulk_get(
            self.VARIABLE, [doc_key, "no-such-key-xyz"],
        )
        assert doc_key in result
        assert "no-such-key-xyz" not in result

        await client.kv.delete(self.VARIABLE, doc_key)

    # ── Bulk put ─────────────────────────────────────────────────────

    async def test_bulk_put(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        items = [
            {"data": {"n": i}, "key": f"abp-{prefix}-{i}"}
            for i in range(4)
        ]
        keys = await client.kv.bulk_put(self.VARIABLE, items)
        assert len(keys) == 4

        docs = await client.kv.bulk_get(self.VARIABLE, keys)
        assert len(docs) == 4

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)

    async def test_bulk_put_auto_keys(self, client: AsyncIyreeClient):
        items = [{"data": {"auto": True}} for _ in range(3)]
        keys = await client.kv.bulk_put(self.VARIABLE, items)
        assert len(keys) == 3
        assert all(len(k) > 0 for k in keys)

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)

    async def test_bulk_put_with_indexes_and_list(self, client: AsyncIyreeClient):
        prefix = uuid.uuid4().hex[:8]
        items = [
            {"data": {"v": "x"}, "key": f"abpi-{prefix}-x", "indexes": {"level": "high"}},
            {"data": {"v": "y"}, "key": f"abpi-{prefix}-y", "indexes": {"level": "low"}},
        ]
        keys = await client.kv.bulk_put(self.VARIABLE, items)

        result = await client.kv.list(
            self.VARIABLE,
            where=[{"index_name": "level", "op": "eq", "value": "high"}],
        )
        found = {d.key for d in result.items}
        assert f"abpi-{prefix}-x" in found
        assert f"abpi-{prefix}-y" not in found

        for k in keys:
            await client.kv.delete(self.VARIABLE, k)
