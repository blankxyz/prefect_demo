from __future__ import annotations

import sys
from pathlib import Path

# 确保 spiders/ 目录在 import 路径中
sys.path.insert(0, str(Path(__file__).parent))

from git_source import get_git_source
from platform_ops import register_spider_deployment, register_spider_from_git

if __name__ == "__main__":
    source = get_git_source()

    # 注册向后兼容的 v1 flow（执行 deploy 脚本）
    register_spider_deployment.from_source(
        source=source,
        entrypoint="spiders/platform_ops.py:register_spider_deployment",
    ).deploy(
        name="platform-register-deployment",
        work_pool_name="docker-crawler-pool",
        description="平台运维：通过 deploy 脚本注册爬虫（向后兼容）",
        tags=["platform", "ops", "deploy"],
    )

    # 注册 v2 flow（参数化注册，无需 deploy 脚本）
    register_spider_from_git.from_source(
        source=source,
        entrypoint="spiders/platform_ops.py:register_spider_from_git",
    ).deploy(
        name="platform-register-from-git",
        work_pool_name="docker-crawler-pool",
        description="平台运维：通过参数从 git 注册爬虫部署（推荐）",
        tags=["platform", "ops", "deploy", "git"],
    )
