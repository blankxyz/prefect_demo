import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from sync_registry import (
    HASHES_VARIABLE,
    _compute_deployment_hash,
    load_deployment_hashes,
    save_deployment_hashes,
    upsert_deployment,
)

BASE_DEP = {
    "name": "spider-a",
    "entrypoint": "spiders/foo.py:foo_flow",
    "work_pool": "docker-crawler-pool",
    "git_branch": "main",
    "description": "测试",
    "tags": ["managed-by:sync", "gdj"],
}


# ── _compute_deployment_hash ────────────────────────────────────────────────


def test_hash_is_deterministic():
    assert _compute_deployment_hash(BASE_DEP) == _compute_deployment_hash(BASE_DEP)


def test_hash_excludes_name():
    dep_b = {**BASE_DEP, "name": "spider-b"}
    assert _compute_deployment_hash(BASE_DEP) == _compute_deployment_hash(dep_b)


def test_hash_sensitive_to_description():
    dep_changed = {**BASE_DEP, "description": "new desc"}
    assert _compute_deployment_hash(BASE_DEP) != _compute_deployment_hash(dep_changed)


def test_hash_sensitive_to_interval():
    dep_with = {**BASE_DEP, "interval_seconds": 86400}
    dep_without = {**BASE_DEP}
    assert _compute_deployment_hash(dep_with) != _compute_deployment_hash(dep_without)


def test_hash_tag_order_independent():
    dep_a = {**BASE_DEP, "tags": ["managed-by:sync", "gdj"]}
    dep_b = {**BASE_DEP, "tags": ["gdj", "managed-by:sync"]}
    assert _compute_deployment_hash(dep_a) == _compute_deployment_hash(dep_b)


# ── load_deployment_hashes ──────────────────────────────────────────────────


def test_load_deployment_hashes_returns_empty_when_missing():
    with patch("sync_registry.Variable") as mock_var, \
         patch("sync_registry.get_run_logger", return_value=MagicMock()):
        mock_var.get.return_value = None
        result = load_deployment_hashes.fn()
    assert result == {}
    mock_var.get.assert_called_once_with(HASHES_VARIABLE, default=None)


def test_load_deployment_hashes_parses_json():
    stored = {"spider-a": "abc123", "spider-b": "def456"}
    with patch("sync_registry.Variable") as mock_var, \
         patch("sync_registry.get_run_logger", return_value=MagicMock()):
        mock_var.get.return_value = json.dumps(stored)
        result = load_deployment_hashes.fn()
    assert result == stored


# ── save_deployment_hashes ──────────────────────────────────────────────────


def test_save_deployment_hashes_serializes_to_json():
    hashes = {"spider-a": "abc123"}
    with patch("sync_registry.Variable") as mock_var, \
         patch("sync_registry.get_run_logger", return_value=MagicMock()):
        save_deployment_hashes.fn(hashes)
    mock_var.set.assert_called_once_with(
        HASHES_VARIABLE,
        json.dumps(hashes, ensure_ascii=False),
        overwrite=True,
    )


# ── 主流程逻辑（不依赖 Prefect flow 运行时）────────────────────────────────


def _make_dep(name: str, interval: int = 86400) -> dict:
    return {
        "name": name,
        "entrypoint": f"spiders/{name}.py:{name}_flow",
        "work_pool": "docker-crawler-pool",
        "git_branch": "main",
        "description": "",
        "tags": ["managed-by:sync"],
        "interval_seconds": interval,
    }


def test_upsert_only_changed_deployments():
    dep_unchanged = _make_dep("spider-a", interval=86400)
    dep_changed = _make_dep("spider-b", interval=3600)

    existing_hashes = {
        "spider-a": _compute_deployment_hash(dep_unchanged),
        "spider-b": _compute_deployment_hash(_make_dep("spider-b", 86400)),
    }

    upsert_mock = MagicMock(return_value="fake-id")
    desired = [dep_unchanged, dep_changed]

    upserted = []
    skipped = []
    new_hashes = dict(existing_hashes)

    for dep in desired:
        name = dep["name"]
        new_hash = _compute_deployment_hash(dep)
        if existing_hashes.get(name) == new_hash:
            skipped.append(name)
            continue
        upsert_mock(dep)
        new_hashes[name] = new_hash
        upserted.append(name)

    assert upserted == ["spider-b"]
    assert skipped == ["spider-a"]
    assert upsert_mock.call_count == 1
    assert new_hashes["spider-b"] == _compute_deployment_hash(dep_changed)
    assert new_hashes["spider-a"] == existing_hashes["spider-a"]


def test_hash_sensitive_to_anchor_time():
    dep_without = {**BASE_DEP, "interval_seconds": 86400}
    dep_with = {**dep_without, "anchor_time": "08:00"}
    assert _compute_deployment_hash(dep_without) != _compute_deployment_hash(dep_with)


def test_hash_anchor_time_value_matters():
    base = {**BASE_DEP, "interval_seconds": 86400}
    dep_08 = {**base, "anchor_time": "08:00"}
    dep_10 = {**base, "anchor_time": "10:00"}
    assert _compute_deployment_hash(dep_08) != _compute_deployment_hash(dep_10)


def test_upsert_deployment_passes_anchor_date():
    dep = {
        "name": "test-flow",
        "entrypoint": "spiders/test.py:test_flow",
        "work_pool": "docker-crawler-pool",
        "git_branch": "main",
        "description": "",
        "tags": ["managed-by:sync"],
        "interval_seconds": 86400,
        "anchor_time": "08:00",
    }
    mock_flow = MagicMock()
    mock_flow.from_source.return_value.deploy.return_value = "fake-id"

    with patch("sync_registry.get_git_source"), \
         patch("sync_registry.get_run_logger", return_value=MagicMock()), \
         patch("sync_registry.Flow", mock_flow):
        upsert_deployment.fn(dep)

    _, call_kwargs = mock_flow.from_source.return_value.deploy.call_args
    assert call_kwargs["anchor_date"] == datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_paused_deployments_removed_from_hashes():
    new_hashes = {"spider-a": "abc", "spider-b": "def"}
    paused = ["spider-b"]
    for name in paused:
        new_hashes.pop(name, None)
    assert "spider-b" not in new_hashes
    assert "spider-a" in new_hashes
