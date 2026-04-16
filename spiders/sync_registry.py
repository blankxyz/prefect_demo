"""轮询 Git 中的 registry.yaml，自动同步爬虫 deployments 到 Prefect。

设计要点：
- 幂等：相同 registry.yaml 多次执行结果一致
- 安全标签：只管理带 managed-by:sync 标签的 deployments
- commit SHA 缓存：无变更时跳过，避免重复操作
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.variables import Variable

from git_source import get_git_source

SYNC_TAG = "managed-by:sync"
SHA_VARIABLE = "registry_last_sha"


@task
def fetch_registry_from_git(git_branch: str = "main") -> dict[str, Any]:
    """从 Git 仓库克隆并读取 registry.yaml，返回解析内容和 commit SHA。"""
    logger = get_run_logger()
    repo_url = os.getenv("GIT_REPO_URL", "https://github.com/blankxyz/prefect_demo.git")

    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", git_branch, repo_url, tmpdir],
            check=True,
            capture_output=True,
            text=True,
        )

        registry_path = Path(tmpdir) / "spiders" / "registry.yaml"
        if not registry_path.is_file():
            # 尝试 prefect_demo/spiders/ 路径
            registry_path = Path(tmpdir) / "prefect_demo" / "spiders" / "registry.yaml"

        if not registry_path.is_file():
            raise FileNotFoundError(f"registry.yaml not found in repo ({git_branch})")

        content = registry_path.read_text(encoding="utf-8")
        content_sha = hashlib.sha256(content.encode()).hexdigest()[:12]

        result = subprocess.run(
            ["git", "-C", tmpdir, "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        commit_sha = result.stdout.strip()[:12]

    registry = yaml.safe_load(content)
    logger.info("从 Git 读取 registry.yaml (commit=%s, content_sha=%s)", commit_sha, content_sha)
    return {
        "registry": registry,
        "commit_sha": commit_sha,
        "content_sha": content_sha,
    }


@task
def check_sha_changed(content_sha: str) -> bool:
    """检查 registry.yaml 内容是否有变更。"""
    logger = get_run_logger()
    last_sha = Variable.get(SHA_VARIABLE, default=None)
    if last_sha == content_sha:
        logger.info("registry.yaml 无变更 (sha=%s)，跳过同步", content_sha)
        return False
    logger.info("检测到变更: %s -> %s", last_sha, content_sha)
    return True


@task
def save_sha(content_sha: str) -> None:
    """保存当前 content SHA 到 Prefect Variable。"""
    Variable.set(SHA_VARIABLE, content_sha, overwrite=True)


def _build_desired_deployments(registry: dict[str, Any]) -> list[dict[str, Any]]:
    """从 registry 配置构建期望的 deployment 列表。"""
    defaults = registry.get("defaults", {})
    default_pool = defaults.get("work_pool", "docker-crawler-pool")
    default_branch = defaults.get("git_branch", "main")

    deployments: list[dict[str, Any]] = []

    for spider in registry.get("spiders", []):
        tags = list(spider.get("tags", []))
        tags.append(SYNC_TAG)

        dep: dict[str, Any] = {
            "entrypoint": spider["entrypoint"],
            "name": spider["name"],
            "work_pool": spider.get("work_pool", default_pool),
            "git_branch": spider.get("git_branch", default_branch),
            "description": spider.get("description", ""),
            "tags": sorted(set(tags)),
        }

        if "interval" in spider:
            dep["interval_seconds"] = spider["interval"]
        elif "cron" in spider:
            dep["cron"] = spider["cron"]

        deployments.append(dep)

    # platform flow（无调度）
    for item in registry.get("platform", []):
        tags = list(item.get("tags", []))
        tags.append(SYNC_TAG)
        deployments.append({
            "entrypoint": item["entrypoint"],
            "name": item["name"],
            "work_pool": item.get("work_pool", default_pool),
            "git_branch": item.get("git_branch", default_branch),
            "description": item.get("description", ""),
            "tags": sorted(set(tags)),
        })

    return deployments


@task
async def get_existing_sync_deployments(work_pool: str = "docker-crawler-pool") -> dict[str, Any]:
    """获取 Prefect 中所有带 managed-by:sync 标签的 deployments。"""
    logger = get_run_logger()
    async with get_client() as client:
        deployments = await client.read_deployments(
            deployment_filter={
                "tags": {"all_": [SYNC_TAG]},
            }
        )
    result = {d.name: d for d in deployments}
    logger.info("Prefect 中已有 %d 个 sync-managed deployments", len(result))
    return result


@task
def upsert_deployment(dep: dict[str, Any]) -> str:
    """注册或更新单个 deployment。"""
    logger = get_run_logger()
    source = get_git_source(branch=dep["git_branch"])

    schedule_kwargs: dict[str, Any] = {}
    if "interval_seconds" in dep:
        schedule_kwargs["interval"] = timedelta(seconds=dep["interval_seconds"])
    elif "cron" in dep:
        schedule_kwargs["cron"] = dep["cron"]

    from prefect import Flow

    deployment_id = Flow.from_source(
        source=source,
        entrypoint=dep["entrypoint"],
    ).deploy(
        name=dep["name"],
        work_pool_name=dep["work_pool"],
        description=dep["description"],
        tags=dep["tags"],
        **schedule_kwargs,
    )

    logger.info("已注册/更新 deployment: %s (id=%s)", dep["name"], deployment_id)
    return str(deployment_id)


@task
async def pause_removed_deployments(
    existing: dict[str, Any],
    desired_names: set[str],
) -> list[str]:
    """暂停不在 registry 中但带 sync 标签的 deployments。"""
    logger = get_run_logger()
    paused: list[str] = []

    async with get_client() as client:
        for name, deployment in existing.items():
            if name not in desired_names:
                await client.set_deployment_paused_state(deployment.id, True)
                logger.warning("已暂停 deployment: %s (不在 registry 中)", name)
                paused.append(name)

    return paused


@flow(name="平台运维_同步爬虫注册表", log_prints=True, retries=1, retry_delay_seconds=60)
def sync_spider_registry(git_branch: str = "main") -> dict[str, Any]:
    """轮询 Git registry.yaml，自动同步 deployments。"""
    logger = get_run_logger()

    # 1. 从 Git 拉取 registry.yaml
    fetch_result = fetch_registry_from_git(git_branch=git_branch)
    registry = fetch_result["registry"]
    content_sha = fetch_result["content_sha"]

    # 2. 检查是否有变更
    if not check_sha_changed(content_sha):
        return {"status": "skipped", "reason": "no_change", "sha": content_sha}

    # 3. 构建期望的 deployment 列表
    desired = _build_desired_deployments(registry)
    desired_names = {d["name"] for d in desired}
    logger.info("registry 中声明了 %d 个 deployments", len(desired))

    # 4. 获取当前 sync-managed deployments
    existing = get_existing_sync_deployments()

    # 5. 注册/更新所有 deployments
    upserted: list[str] = []
    for dep in desired:
        dep_id = upsert_deployment(dep)
        upserted.append(dep["name"])

    # 6. 暂停已移除的 deployments
    paused = pause_removed_deployments(existing, desired_names)

    # 7. 保存 SHA
    save_sha(content_sha)

    summary = {
        "status": "synced",
        "sha": content_sha,
        "upserted": upserted,
        "paused": paused,
    }
    logger.info("同步完成: %s", summary)
    return summary


if __name__ == "__main__":
    sync_spider_registry()
