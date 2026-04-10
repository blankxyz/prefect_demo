from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, time, timezone
from typing import Any

import asyncpg
from prefect import get_run_logger


RESULTS_TABLE = "spider_results"

_RESERVED_KEYS = {
    "spider_name",
    "source_name",
    "item_type",
    "url",
    "published_at",
    "published_date",
    "dedupe_key",
    "raw_data",
}


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _get_logger():
    try:
        return get_run_logger()
    except Exception:  # pragma: no cover - local scripts
        return logging.getLogger(__name__)


def _to_asyncpg_dsn(raw_url: str | None) -> str | None:
    if not raw_url:
        return None
    return raw_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _pick_db_url(explicit_db_url: str | None = None) -> str | None:
    return _to_asyncpg_dsn(
        explicit_db_url
        or os.getenv("SPIDER_RESULTS_DB_URL")
        or os.getenv("GDJ_RESULTS_DB_URL")
        or os.getenv("PREFECT_API_DATABASE_CONNECTION_URL")
    )


def _to_published_at(value: Any) -> datetime | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None

    normalized = normalized.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    try:
        day = date.fromisoformat(normalized[:10])
    except ValueError:
        return None
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def _json_ready(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    return str(value)


def _make_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _make_dedupe_key(source_name: str, item_type: str, url: str, explicit_key: str | None = None) -> str:
    if explicit_key:
        return explicit_key
    raw = f"{source_name}|{item_type}|{url}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_record(
    record: dict[str, Any],
    *,
    spider_name: str,
    flow_run_id: str | None,
    default_source_name: str | None = None,
    default_item_type: str = "item",
) -> dict[str, Any] | None:
    url = _normalize_text(record.get("url"))
    if not url:
        return None

    source_name = _normalize_text(record.get("source_name") or default_source_name or spider_name)
    item_type = _normalize_text(record.get("item_type") or default_item_type)
    explicit_dedupe_key = _normalize_text(record.get("dedupe_key")) or None

    data = {
        key: _json_ready(value)
        for key, value in record.items()
        if key not in _RESERVED_KEYS
    }
    raw_data = _json_ready(record.get("raw_data"))

    return {
        "spider_name": spider_name,
        "flow_run_id": flow_run_id or "local-run",
        "source_name": source_name,
        "item_type": item_type,
        "url": url,
        "url_hash": _make_url_hash(url),
        "dedupe_key": _make_dedupe_key(source_name, item_type, url, explicit_dedupe_key),
        "published_at": _to_published_at(record.get("published_at") or record.get("published_date") or record.get("date")),
        "data": data,
        "raw_data": raw_data,
    }


def _table_sql(table_name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        item_id BIGSERIAL PRIMARY KEY,
        spider_name TEXT NOT NULL,
        flow_run_id TEXT NOT NULL,
        source_name TEXT NOT NULL,
        item_type TEXT NOT NULL,
        url TEXT NOT NULL,
        url_hash TEXT NOT NULL,
        dedupe_key TEXT NOT NULL,
        published_at TIMESTAMPTZ NULL,
        data JSONB NOT NULL DEFAULT '{{}}'::jsonb,
        raw_data JSONB NULL,
        crawled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """


def _index_sql(table_name: str) -> list[str]:
    return [
        f"CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_run_dedupe_idx ON {table_name} (spider_name, flow_run_id, dedupe_key)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_spider_time_idx ON {table_name} (spider_name, crawled_at DESC)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_source_time_idx ON {table_name} (source_name, crawled_at DESC)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_item_type_time_idx ON {table_name} (item_type, crawled_at DESC)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_url_hash_idx ON {table_name} (url_hash)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_published_at_idx ON {table_name} (published_at DESC)",
        f"CREATE INDEX IF NOT EXISTS {table_name}_data_gin_idx ON {table_name} USING GIN (data)",
    ]


def _upsert_sql(table_name: str) -> str:
    return f"""
    INSERT INTO {table_name} (
        spider_name,
        flow_run_id,
        source_name,
        item_type,
        url,
        url_hash,
        dedupe_key,
        published_at,
        data,
        raw_data,
        crawled_at,
        updated_at
    )
    VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, NOW(), NOW()
    )
    ON CONFLICT (spider_name, flow_run_id, dedupe_key) DO UPDATE SET
        source_name = EXCLUDED.source_name,
        item_type = EXCLUDED.item_type,
        url = EXCLUDED.url,
        url_hash = EXCLUDED.url_hash,
        published_at = EXCLUDED.published_at,
        data = EXCLUDED.data,
        raw_data = EXCLUDED.raw_data,
        updated_at = NOW()
    """


async def store_spider_results(
    records: list[dict[str, Any]],
    *,
    spider_name: str,
    flow_run_id: str | None = None,
    db_url: str | None = None,
    table_name: str = RESULTS_TABLE,
    default_source_name: str | None = None,
    default_item_type: str = "item",
) -> int:
    logger = _get_logger()
    resolved_db_url = _pick_db_url(db_url)
    if not resolved_db_url:
        logger.info("未配置数据库连接，跳过落库")
        return 0

    rows = []
    for record in records:
        normalized = normalize_record(
            record,
            spider_name=spider_name,
            flow_run_id=flow_run_id,
            default_source_name=default_source_name,
            default_item_type=default_item_type,
        )
        if normalized:
            rows.append(normalized)

    if not rows:
        logger.info("没有可写入的有效记录")
        return 0

    conn = await asyncpg.connect(resolved_db_url)
    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", table_name)
            await conn.execute(_table_sql(table_name))
            for sql in _index_sql(table_name):
                await conn.execute(sql)

        await conn.executemany(
            _upsert_sql(table_name),
            [
                (
                    row["spider_name"],
                    row["flow_run_id"],
                    row["source_name"],
                    row["item_type"],
                    row["url"],
                    row["url_hash"],
                    row["dedupe_key"],
                    row["published_at"],
                    json.dumps(row["data"], ensure_ascii=False),
                    json.dumps(row["raw_data"], ensure_ascii=False) if row["raw_data"] is not None else None,
                )
                for row in rows
            ],
        )
        return len(rows)
    finally:
        await conn.close()


def store_spider_results_sync(
    records: list[dict[str, Any]],
    *,
    spider_name: str,
    flow_run_id: str | None = None,
    db_url: str | None = None,
    table_name: str = RESULTS_TABLE,
    default_source_name: str | None = None,
    default_item_type: str = "item",
) -> int:
    return asyncio.run(
        store_spider_results(
            records,
            spider_name=spider_name,
            flow_run_id=flow_run_id,
            db_url=db_url,
            table_name=table_name,
            default_source_name=default_source_name,
            default_item_type=default_item_type,
        )
    )


async def spider_result_count(
    spider_name: str,
    *,
    db_url: str | None = None,
    table_name: str = RESULTS_TABLE,
) -> int:
    resolved_db_url = _pick_db_url(db_url)
    if not resolved_db_url:
        return 0

    conn = await asyncpg.connect(resolved_db_url)
    try:
        return await conn.fetchval(
            f"SELECT COUNT(*) FROM {table_name} WHERE spider_name = $1",
            spider_name,
        ) or 0
    finally:
        await conn.close()


def spider_result_count_sync(
    spider_name: str,
    *,
    db_url: str | None = None,
    table_name: str = RESULTS_TABLE,
) -> int:
    return asyncio.run(
        spider_result_count(
            spider_name,
            db_url=db_url,
            table_name=table_name,
        )
    )
