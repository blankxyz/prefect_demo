from __future__ import annotations

import os
import json
import time
from datetime import datetime
from typing import Any

from prefect import get_run_logger, task

from common.clickhouse_sink import insert_rows
from common.kafka_sink import publish_records

RUNTIME_FLAGS_PATH = os.getenv("RUNTIME_FLAGS_PATH", "/app/runtime/system_flags.json")


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _read_runtime_flags() -> dict[str, Any]:
    try:
        with open(RUNTIME_FLAGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _runtime_flag(name: str, default: bool = True) -> bool:
    data = _read_runtime_flags()
    raw = data.get(name)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def build_clickhouse_rows(
    items: list[dict[str, Any]],
    *,
    site_name: str,
    crawl_time: datetime | None = None,
    version: int | None = None,
) -> list[list[Any]]:
    if not items:
        return []

    resolved_crawl_time = crawl_time or datetime.now()
    resolved_version = version or time.time_ns()
    return [
        [item.get("url", ""), site_name, item, resolved_crawl_time, resolved_version]
        for item in items
    ]


@task(name="保存抓取结果", retries=3, retry_delay_seconds=10)
def save_items_to_sinks(
    items: list[dict[str, Any]],
    *,
    site_name: str,
    topic: str,
    bootstrap_servers: list[str],
    table: str = "crawler_data",
    enable_kafka: bool | None = None,
) -> int:
    logger = get_run_logger()
    rows = build_clickhouse_rows(items, site_name=site_name)
    if not rows:
        logger.info("没有可保存的数据")
        return 0

    insert_rows(rows, table=table, logger=logger)

    if enable_kafka is None:
        env_default = _env_flag("ENABLE_KAFKA", default=True)
        resolved_enable_kafka = _runtime_flag("enable_kafka", default=env_default)
    else:
        resolved_enable_kafka = enable_kafka
    if resolved_enable_kafka:
        publish_records(items, topic=topic, bootstrap_servers=bootstrap_servers, logger=logger)
    else:
        logger.info("Kafka 写入已关闭，跳过推送 topic=%s", topic)
    return len(rows)
