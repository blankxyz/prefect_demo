from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from gdj_video_spider import gdj_video_flow


if __name__ == "__main__":
    gdj_video_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="gdj_video_spider.py:gdj_video_flow",
    ).deploy(
        name="gdj-video-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=7),
        description="甘肃广电局陇上精品视频抓取",
        tags=["gdj", "crawl4ai", "video"],
    )
