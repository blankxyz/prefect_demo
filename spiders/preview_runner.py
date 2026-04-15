"""预览沙箱：在 worker 内以 monkey-patch 方式拦截数据写入，返回前 5 条结果。"""
from __future__ import annotations

import importlib
import json
from pathlib import Path
from unittest.mock import patch

from prefect import flow
from prefect.runtime import flow_run

PROJECT_ROOT = Path("/workspace")


@flow(name="preview-runner", log_prints=True)
def preview_runner_flow(module_slug: str) -> None:
    run_id = str(flow_run.id)
    output_path = PROJECT_ROOT / "runtime" / f"preview_{run_id}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    captured: list[dict] = []

    def _capture_records(records, *args, **kw) -> None:
        """拦截 save_items_to_sinks / save_data / insert_rows 调用，收集数据。"""
        if isinstance(records, list):
            for r in records:
                if isinstance(r, dict):
                    captured.append(r)
        elif isinstance(records, dict):
            captured.append(records)

    def _passthrough_filter(items, **kw):
        """跳过 ClickHouse 去重，直接返回全部 items。"""
        return items if isinstance(items, list) else []

    try:
        # Patch 必须在 importlib.import_module 之前生效，
        # 这样 spider 模块的 from X import Y 会绑定到 mock 函数。
        with (
            patch("common.result_sink.save_items_to_sinks", _capture_records),
            patch("common.clickhouse_sink.filter_new_items_by_url", _passthrough_filter),
            patch("common.clickhouse_sink.save_data", _capture_records),
            patch("common.clickhouse_sink.insert_rows", _capture_records),
        ):
            mod = importlib.import_module(f"spiders.{module_slug}_spider")
            spider_flow_fn = getattr(mod, f"{module_slug}_flow")
            spider_flow_fn()

        output: dict = {
            "module_slug": module_slug,
            "results": captured[:5],
            "count": len(captured),
        }
    except Exception as exc:
        output = {
            "module_slug": module_slug,
            "error": str(exc),
            "results": captured[:5],
            "count": len(captured),
        }

    output_path.write_text(
        json.dumps(output, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"preview done: {len(captured)} records captured → {output_path}")
