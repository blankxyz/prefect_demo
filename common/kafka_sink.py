from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer
from prefect import get_run_logger, task


def publish_records(
    records: list[dict[str, Any]],
    *,
    topic: str,
    bootstrap_servers: list[str],
    key_field: str | None = "url",
    logger=None,
) -> int:
    if not records:
        if logger:
            logger.info("没有可推送的 Kafka 记录")
        return 0

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=3,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )

    sent = 0
    try:
        for record in records:
            key = None
            if key_field:
                raw_key = record.get(key_field)
                key = str(raw_key) if raw_key else None
            producer.send(topic, key=key, value=record)
            sent += 1

        producer.flush()
        if logger:
            logger.info("成功推送 %s 条数据到 Kafka topic=%s", sent, topic)
        return sent
    finally:
        producer.close()


@task(name="推送数据到Kafka", retries=3, retry_delay_seconds=10)
def publish_json_records(
    records: list[dict[str, Any]],
    *,
    topic: str,
    bootstrap_servers: list[str],
    key_field: str | None = "url",
) -> int:
    logger = get_run_logger()
    try:
        return publish_records(
            records,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            key_field=key_field,
            logger=logger,
        )
    except Exception:
        logger.exception("推送 Kafka 失败，topic=%s", topic)
        raise
