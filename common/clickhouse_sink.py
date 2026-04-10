from __future__ import annotations

import os
from typing import Any

import clickhouse_connect
from prefect import get_run_logger, task


def _get_client():
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    return clickhouse_connect.get_client(host=host, username="default", password="")


def _sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _load_existing_urls(site_name: str, urls: list[str], batch_size: int = 200) -> set[str]:
    if not urls:
        return set()

    client = _get_client()
    existing: set[str] = set()
    try:
        for start in range(0, len(urls), batch_size):
            batch = urls[start : start + batch_size]
            in_clause = ", ".join(_sql_quote(url) for url in batch)
            query = (
                "SELECT DISTINCT url FROM crawler_data "
                f"WHERE site_name = {_sql_quote(site_name)} AND url IN ({in_clause})"
            )
            result = client.query(query)
            existing.update(str(row[0]) for row in result.result_rows)
        return existing
    finally:
        client.close()


def insert_rows(data_list, table: str = "crawler_data", logger=None) -> int:
    if not data_list:
        if logger:
            logger.info("没有可写入的数据")
        return 0

    client = _get_client()
    try:
        client.insert(table, data_list)
        if logger:
            logger.info(f"成功写入 {len(data_list)} 条数据至 {table}")
        return len(data_list)
    finally:
        client.close()


@task(name="按URL过滤历史数据", retries=3, retry_delay_seconds=10)
def filter_new_items_by_url(
    items: list[dict[str, Any]],
    *,
    site_name: str,
    url_field: str = "url",
) -> list[dict[str, Any]]:
    logger = get_run_logger()
    if not items:
        logger.info("没有可过滤的数据")
        return []

    deduped_items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    duplicate_in_batch = 0

    for item in items:
        url = str(item.get(url_field) or "").strip()
        if not url:
            continue
        if url in seen_urls:
            duplicate_in_batch += 1
            continue
        seen_urls.add(url)
        deduped_items.append(item)

    existing_urls = _load_existing_urls(site_name, list(seen_urls))
    new_items = [item for item in deduped_items if str(item.get(url_field) or "").strip() not in existing_urls]

    logger.info(
        "URL过滤完成：输入 %s 条，批内重复 %s 条，历史已存在 %s 条，保留 %s 条",
        len(items),
        duplicate_in_batch,
        len(existing_urls),
        len(new_items),
    )
    return new_items

@task(name="数据落库", retries=3, retry_delay_seconds=30)
def save_data(data_list, table="crawler_data"):
    """
    data_list: list of dicts/tuples
    """
    logger = get_run_logger()
    
    try:
        return insert_rows(data_list, table=table, logger=logger)
    except Exception as e:
        logger.error(f"写入失败: {e}")
        raise
