from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger

from git_source import get_git_source


@flow(name="平台运维_注册爬虫部署", log_prints=True)
def register_spider_deployment(deploy_script: str) -> dict[str, str]:
    """向后兼容：通过执行 deploy 脚本注册 deployment。"""
    logger = get_run_logger()
    project_root = Path(os.getenv("PROJECT_ROOT", "/app")).resolve()
    script_path = (project_root / deploy_script).resolve()

    if project_root not in script_path.parents:
        raise ValueError(f"部署脚本超出项目目录: {deploy_script}")
    if not script_path.is_file():
        raise FileNotFoundError(f"部署脚本不存在: {script_path}")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        logger.info("deploy stdout:\n%s", result.stdout.strip())
    if result.stderr.strip():
        logger.warning("deploy stderr:\n%s", result.stderr.strip())

    if result.returncode != 0:
        raise RuntimeError(f"部署脚本执行失败: {deploy_script}")

    logger.info("部署脚本执行成功: %s", deploy_script)
    return {"deploy_script": deploy_script, "status": "ok"}


@flow(name="平台运维_注册爬虫部署_v2", log_prints=True)
def register_spider_from_git(
    entrypoint: str,
    deployment_name: str,
    description: str = "",
    tags: list[str] | None = None,
    interval_seconds: int | None = None,
    cron: str | None = None,
    git_branch: str = "main",
) -> dict[str, str]:
    """通过参数直接注册 deployment，代码从 git 拉取，无需 deploy 脚本。"""
    logger = get_run_logger()

    source = get_git_source(branch=git_branch)

    schedule_kwargs: dict = {}
    if interval_seconds is not None:
        schedule_kwargs["interval"] = timedelta(seconds=interval_seconds)
    elif cron is not None:
        schedule_kwargs["cron"] = cron

    from prefect import Flow

    deployment_id = Flow.from_source(
        source=source,
        entrypoint=entrypoint,
    ).deploy(
        name=deployment_name,
        work_pool_name="docker-crawler-pool",
        description=description,
        tags=tags or [],
        **schedule_kwargs,
    )

    logger.info("部署注册成功: %s (id=%s)", deployment_name, deployment_id)
    return {
        "deployment_name": deployment_name,
        "entrypoint": entrypoint,
        "deployment_id": str(deployment_id),
        "status": "ok",
    }
