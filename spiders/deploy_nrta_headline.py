from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from nrta_headline_spider import nrta_headline_flow
except ImportError:
    from spiders.nrta_headline_spider import nrta_headline_flow


if __name__ == "__main__":
    nrta_headline_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="nrta_headline_spider.py:nrta_headline_flow",
    ).deploy(
        name="nrta-headline-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="国家广播电视总局总局要闻抓取",
        tags=["nrta", "headline", "daily"],
    )
