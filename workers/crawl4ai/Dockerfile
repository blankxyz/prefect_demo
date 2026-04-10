# syntax=docker/dockerfile:1.7

# Crawl4AI 对浏览器和系统库依赖比较敏感，固定到 bookworm 更稳一些
FROM python:3.11-slim-bookworm

WORKDIR /app

# 避免交互式提示干扰 Docker 构建
ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# 安装必要的系统工具和 Python 依赖
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update \
    && apt-get install -y --no-install-recommends wget gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install prefect==3.6.23 crawl4ai \
    && crawl4ai-setup \
    && playwright install --with-deps chromium

# 启动 Worker，并告诉它只盯着 "crawl4ai-pool" 这个任务池
CMD ["prefect", "worker", "start", "--pool", "crawl4ai-pool"]
