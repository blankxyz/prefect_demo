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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import json

import yaml
from prefect import Flow, flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import IntervalSchedule
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.variables import Variable

from git_source import get_git_source

SYNC_TAG = "managed-by:sync"
SHA_VARIABLE = "registry_last_sha"
HASHES_VARIABLE = "registry_deployment_hashes"
HASH_FIELDS = ("entrypoint", "work_pool", "git_branch", "description", "tags", "interval_seconds", "cron", "anchor_time")


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


@task
def load_deployment_hashes() -> dict[str, str]:
    """从 Prefect Variable 加载 deployment hashes，首次运行返回空 dict。"""
    logger = get_run_logger()
    raw = Variable.get(HASHES_VARIABLE, default=None)
    if raw is None:
        logger.info("未找到 %s，视为首次运行", HASHES_VARIABLE)
        return {}
    result: dict[str, str] = json.loads(raw)
    logger.info("已加载 %d 个 deployment hashes", len(result))
    return result


@task
def save_deployment_hashes(hashes: dict[str, str]) -> None:
    """保存 deployment hashes dict 到 Prefect Variable。"""
    logger = get_run_logger()
    Variable.set(HASHES_VARIABLE, json.dumps(hashes, ensure_ascii=False), overwrite=True)
    logger.info("已保存 %d 个 deployment hashes", len(hashes))


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

        if "anchor_time" in spider:
            dep["anchor_time"] = spider["anchor_time"]

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


def _compute_deployment_hash(dep: dict[str, Any]) -> str:
    """计算 deployment 配置的稳定哈希（不含 name 字段），用于变更检测。"""
    payload = {k: dep[k] for k in HASH_FIELDS if k in dep}
    if "tags" in payload:
        payload = {**payload, "tags": sorted(payload["tags"])}
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]


@task
def get_existing_sync_deployments() -> dict[str, Any]:
    """获取 Prefect 中所有带 managed-by:sync 标签的 deployments。"""
    logger = get_run_logger()

    async def _fetch():
        async with get_client() as client:
            return await client.read_deployments(
                deployment_filter={"tags": {"all_": [SYNC_TAG]}}
            )

    deployments = run_coro_as_sync(_fetch())
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
        interval = timedelta(seconds=dep["interval_seconds"])
        if "anchor_time" in dep:
            hour, minute = (int(x) for x in dep["anchor_time"].split(":"))
            anchor = datetime(2024, 1, 1, hour, minute, tzinfo=timezone.utc)
            schedule_kwargs["schedule"] = IntervalSchedule(interval=interval, anchor_date=anchor)
        else:
            schedule_kwargs["interval"] = interval
    elif "cron" in dep:
        schedule_kwargs["cron"] = dep["cron"]

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
def pause_removed_deployments(
    existing: dict[str, Any],
    desired_names: set[str],
) -> list[str]:
    """暂停不在 registry 中但带 sync 标签的 deployments。"""
    logger = get_run_logger()
    to_pause = [name for name in existing if name not in desired_names]

    async def _pause():
        async with get_client() as client:
            for name in to_pause:
                await client.set_deployment_paused_state(existing[name].id, True)

    if to_pause:
        run_coro_as_sync(_pause())
        for name in to_pause:
            logger.warning("已暂停 deployment: %s (不在 registry 中)", name)

    return to_pause


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

    # 4. 获取当前 sync-managed deployments（用于暂停检测）
    existing = get_existing_sync_deployments()

    # 5. 加载 per-deployment hashes，只 upsert 有变更的
    current_hashes = load_deployment_hashes()
    new_hashes = dict(current_hashes)

    upserted: list[str] = []
    skipped: list[str] = []

    for dep in desired:
        name = dep["name"]
        new_hash = _compute_deployment_hash(dep)
        if current_hashes.get(name) == new_hash:
            skipped.append(name)
            logger.debug("跳过未变更的 deployment: %s (hash=%s)", name, new_hash)
            continue
        upsert_deployment(dep)
        new_hashes[name] = new_hash
        upserted.append(name)

    # 6. 暂停已移除的 deployments
    paused = pause_removed_deployments(existing, desired_names)

    # 7. 清除已暂停 deployment 的 hash（重新加回时应触发 upsert）
    for name in paused:
        new_hashes.pop(name, None)

    # 8. 保存 hashes 和文件级 SHA
    save_deployment_hashes(new_hashes)
    save_sha(content_sha)

    summary = {
        "status": "synced",
        "sha": content_sha,
        "upserted": upserted,
        "skipped": skipped,
        "paused": paused,
    }
    logger.info("同步完成: %s", summary)
    return summary


if __name__ == "__main__":
    sync_spider_registry()
