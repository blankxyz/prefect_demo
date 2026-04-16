from __future__ import annotations

from datetime import timedelta

from git_source import get_git_source


if __name__ == "__main__":
    from hinews_shixian_all_spider import hinews_shixian_all_flow

    hinews_shixian_all_flow.from_source(
        source=get_git_source(),
        entrypoint="spiders/hinews_shixian_all_spider.py:hinews_shixian_all_flow",
    ).deploy(
        name="hinews-shixian-all-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="海南Hinews全部市县第一页抓取",
        tags=["hinews", "shixian", "daily", "git"],
    )
