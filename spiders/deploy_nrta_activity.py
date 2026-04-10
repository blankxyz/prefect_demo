from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from nrta_activity_spider import nrta_activity_flow
except ImportError:
    from spiders.nrta_activity_spider import nrta_activity_flow


if __name__ == "__main__":
    nrta_activity_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="nrta_activity_spider.py:nrta_activity_flow",
    ).deploy(
        name="nrta-activity-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="国家广播电视总局工作动态抓取",
        tags=["nrta", "activity", "daily"],
    )
