# syntax=docker/dockerfile:1.7

# 使用轻量级 Python 镜像
FROM python:3.11-slim-bookworm

WORKDIR /app

# 安装和控制平面一致的 Prefect 版本，避免 client/server 版本不匹配
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install prefect==3.6.23 scrapling[all]


# 启动 Worker，并告诉它只盯着 "scrapling-pool" 这个任务池
CMD ["prefect", "worker", "start", "--pool", "scrapling-pool"]
