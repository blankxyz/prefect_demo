from __future__ import annotations

from pathlib import Path

try:
    from preview_runner import preview_runner_flow
except ImportError:
    from spiders.preview_runner import preview_runner_flow


if __name__ == "__main__":
    preview_runner_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="preview_runner.py:preview_runner_flow",
    ).deploy(
        name="preview-runner",
        work_pool_name="docker-crawler-pool",
        description="爬虫代码预览沙箱，拦截数据写入并返回前 5 条结果",
        tags=["preview", "sandbox"],
    )
