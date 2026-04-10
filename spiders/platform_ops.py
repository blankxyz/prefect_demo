from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger


@flow(name="平台运维_注册爬虫部署", log_prints=True)
def register_spider_deployment(deploy_script: str) -> dict[str, str]:
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
