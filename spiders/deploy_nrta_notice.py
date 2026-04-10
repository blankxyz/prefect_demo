from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from nrta_notice_spider import nrta_notice_flow
except ImportError:
    from spiders.nrta_notice_spider import nrta_notice_flow


if __name__ == "__main__":
    nrta_notice_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="nrta_notice_spider.py:nrta_notice_flow",
    ).deploy(
        name="nrta-notice-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="国家广播电视总局公告公示抓取",
        tags=["nrta", "notice", "daily"],
    )
