from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from gdj_henan_tz_spider import gdj_henan_tz_flow
except ImportError:
    from spiders.gdj_henan_tz_spider import gdj_henan_tz_flow


if __name__ == "__main__":
    gdj_henan_tz_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="gdj_henan_tz_spider.py:gdj_henan_tz_flow",
    ).deploy(
        name="gdj-henan-tz-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="河南省广播电视局通知公告详情抓取",
        tags=["gdj", "henan", "tzgg"],
    )
