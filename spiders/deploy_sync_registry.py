"""一次性注册 sync_registry flow 到 Prefect。

运行一次即可，之后 sync flow 每30分钟自动轮询 Git registry.yaml。
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from git_source import get_git_source
from sync_registry import sync_spider_registry

if __name__ == "__main__":
    source = get_git_source()

    sync_spider_registry.from_source(
        source=source,
        entrypoint="spiders/sync_registry.py:sync_spider_registry",
    ).deploy(
        name="sync-spider-registry",
        work_pool_name="docker-crawler-pool",
        interval=timedelta(minutes=30),
        description="每30分钟轮询 Git registry.yaml，自动同步爬虫部署",
        tags=["platform", "sync"],
    )

    print("sync-spider-registry deployment 注册成功")
