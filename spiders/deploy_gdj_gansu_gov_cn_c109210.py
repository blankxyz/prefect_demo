from __future__ import annotations

from datetime import timedelta
from pathlib import Path

try:
    from gdj_gansu_gov_cn_c109210_spider import gdj_gansu_gov_cn_c109210_flow
except ImportError:
    from spiders.gdj_gansu_gov_cn_c109210_spider import gdj_gansu_gov_cn_c109210_flow


if __name__ == "__main__":
    gdj_gansu_gov_cn_c109210_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="gdj_gansu_gov_cn_c109210_spider.py:gdj_gansu_gov_cn_c109210_flow",
    ).deploy(
        name="gdj-gansu-c109210-flow",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(days=1),
        description="甘肃省广电局本局消息第一页抓取",
        tags=["gdj", "gansu", "c109210", "daily", "local"],
    )
