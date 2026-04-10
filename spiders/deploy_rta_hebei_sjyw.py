from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from rta_hebei_sjyw_spider import rta_hebei_sjyw_flow
except ImportError:
    from spiders.rta_hebei_sjyw_spider import rta_hebei_sjyw_flow


if __name__ == "__main__":
    rta_hebei_sjyw_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="rta_hebei_sjyw_spider.py:rta_hebei_sjyw_flow",
    ).deploy(
        name="rta_hebei_sjyw-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="河北省广播电视局省局要闻抓取",
        tags=["hebei", "rta", "sjyw", "daily"],
    )
