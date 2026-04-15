from __future__ import annotations

import os

from prefect.runner.storage import GitRepository


def get_git_source(branch: str = "main") -> GitRepository:
    repo_url = os.getenv("GIT_REPO_URL", "https://github.com/<org>/<repo>.git")
    return GitRepository(url=repo_url, branch=branch)
