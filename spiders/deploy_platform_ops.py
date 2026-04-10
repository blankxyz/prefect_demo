from __future__ import annotations

from pathlib import Path

try:
    from platform_ops import register_spider_deployment
except ImportError:
    from spiders.platform_ops import register_spider_deployment


if __name__ == "__main__":
    register_spider_deployment.from_source(
        source=str(Path(__file__).parent),
        entrypoint="platform_ops.py:register_spider_deployment",
    ).deploy(
        name="platform-register-deployment",
        work_pool_name="docker-crawler-pool",
        description="平台运维：由 worker 执行新爬虫 deployment 注册",
        tags=["platform", "ops", "deploy"],
    )
