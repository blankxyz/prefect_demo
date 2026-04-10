from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from gdj_xydt_spider import gdj_xydt_flow
except ImportError:
    from spiders.gdj_xydt_spider import gdj_xydt_flow


if __name__ == "__main__":
    gdj_xydt_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="gdj_xydt_spider.py:gdj_xydt_flow",
    ).deploy(
        name="gdj-xydt-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="辽宁省广播电视局行业动态详情抓取",
        tags=["gdj", "liaoning", "xydt"],
    )
