# 爬虫调度机制文档

## 概述

本项目采用**声明式调度**模式：在 `registry.yaml` 中描述所有爬虫的部署配置，由 `sync_registry` 定时 flow 自动同步到 Prefect。日常维护只需修改配置文件并推送 Git，无需手动操作 Prefect。

---

## 架构总览

```
Git (registry.yaml)
        │
        │  每 30 分钟拉取一次
        ▼
sync-spider-registry (Prefect Flow)
        │
        ├── 文件内容无变化 → 跳过（零 API 调用）
        │
        └── 文件有变化 → 逐个比对 deployment 配置哈希
                │
                ├── 哈希未变 → 跳过该 deployment
                │
                ├── 哈希已变 / 新增 → 调用 Flow.deploy() 注册/更新
                │
                └── 已删除（不在 registry 中）→ 暂停 deployment
```

---

## 关键文件

| 文件 | 职责 |
|------|------|
| `spiders/registry.yaml` | 爬虫调度配置的唯一真相来源，声明所有 deployment |
| `spiders/sync_registry.py` | 同步逻辑主体，Prefect Flow，每 30 分钟执行一次 |
| `spiders/deploy_sync_registry.py` | 一次性脚本，用于向 Prefect 注册 sync-spider-registry deployment 本身 |
| `spiders/git_source.py` | 封装 GitRepository，供所有 deployment 的 `from_source()` 使用 |

---

## registry.yaml 字段说明

```yaml
version: 1

defaults:
  work_pool: docker-crawler-pool   # 所有 spider 默认使用的 work pool
  git_branch: main                 # 所有 spider 默认拉取的分支

spiders:
  - entrypoint: spiders/foo.py:foo_flow   # 必填：Python 文件路径:函数名
    name: foo-flow                         # 必填：Prefect deployment 名称（唯一）
    interval: 86400                        # 调度间隔（秒）；与 cron 二选一
    cron: "0 8 * * *"                      # Cron 表达式；与 interval 二选一
    anchor_time: "08:00"                   # 可选：interval 调度的锚点时间（UTC HH:MM）
    description: 描述文字                  # 可选：显示在 Prefect UI 中
    tags: [tag1, tag2]                     # 可选：附加标签（managed-by:sync 自动追加）
    work_pool: custom-pool                 # 可选：覆盖 defaults.work_pool
    git_branch: develop                    # 可选：覆盖 defaults.git_branch
```

> **注意：** `platform` 区块用于无调度的按需触发 flow，字段结构同 spiders，但不填 interval/cron。

---

## anchor_time 详解

`anchor_time` 控制 interval 调度每天首次触发的时刻，时区为 **UTC**。

| registry.yaml 配置 | 实际效果 |
|--------------------|---------|
| `interval: 86400`（无 anchor_time）| Prefect 自动分配触发时间，可能随机 |
| `interval: 86400` + `anchor_time: "08:00"` | 每天 UTC 08:00（北京时间 16:00）触发 |
| `interval: 86400` + `anchor_time: "01:00"` | 每天 UTC 01:00（北京时间 09:00）触发 |

**实现原理：** `anchor_time` 被转换为 `IntervalSchedule(interval=..., anchor_date=datetime(2024,1,1,HH,MM,tzinfo=UTC))`，Prefect 以此为基准点推算后续触发时间。

---

## 当前爬虫调度表

> 所有 interval 单位为秒，时间为 UTC。北京时间 = UTC + 8。

| deployment 名称 | 触发时间 (UTC) | 触发时间 (CST) | 间隔 | 描述 |
|----------------|--------------|--------------|------|------|
| gdj-henan-tz-flow | 01:00 | 09:00 | 每日 | 河南省广播电视局通知公告 |
| gdj-video-flow | 02:30 | 10:30 | 每周 | 甘肃广电局陇上精品视频 |
| gdj-xydt-flow | 04:00 | 12:00 | 每日 | 辽宁省广播电视局行业动态 |
| nrta-activity-flow | 07:00 | 15:00 | 每日 | 国家广电总局工作动态 |
| nrta-headline-flow | 10:00 | 18:00 | 每日 | 国家广电总局总局要闻 |
| nrta-notice-flow | 13:00 | 21:00 | 每日 | 国家广电总局公告公示 |
| rta-hebei-sjyw-flow | 16:00 | 00:00 | 每日 | 河北省广播电视局省局要闻 |
| hinews-shixian-all-flow | 19:00 | 03:00 | 每日 | 海南新闻市县频道 |
| gdj-gansu-c109210-flow | 22:00 | 06:00 | 每日 | 甘肃省广电局本局消息 |

每日任务间隔 3 小时，避免并发资源争抢。

---

## 变更检测机制

sync_registry 使用两层缓存避免无效 API 调用：

### 第一层：文件级 SHA（快速退出）

- 对整个 `registry.yaml` 文件内容计算 SHA256
- 存储于 Prefect Variable：`registry_last_sha`
- 若文件内容未变化 → 整个 flow 直接返回，不发起任何 Prefect API 请求

### 第二层：per-deployment 配置哈希（精细比对）

- 对每个 deployment 的配置字段（entrypoint / work_pool / git_branch / description / tags / interval_seconds / cron / anchor_time）计算 SHA256
- 全部哈希以 JSON 字典形式存储于 Prefect Variable：`registry_deployment_hashes`
- 若某 deployment 配置未变化 → 跳过该 deployment 的 `upsert`，只处理真正有变更的条目

### 已删除 deployment 的处理

- deployment 从 registry 中删除后不会被直接删除，而是**暂停**（paused）
- 同时从 `registry_deployment_hashes` 中清除其哈希记录
- 若将来重新加回（即便配置完全相同），仍会触发一次 upsert 重新激活

---

## 日常操作指南

### 新增爬虫

1. 在 `registry.yaml` 的 `spiders` 区块添加新条目
2. `git push`
3. 等待 sync flow 自动执行（最长 30 分钟），或手动触发：
   ```bash
   docker exec -e PREFECT_API_URL=http://prefect-server:4200/api \
     prefect_demo-crawler-worker-1 \
     prefect deployment run '平台运维_同步爬虫注册表/sync-spider-registry'
   ```

### 修改爬虫调度时间

编辑 `registry.yaml` 中对应 spider 的 `anchor_time` 或 `interval`，`git push` 即可。

### 暂停某个爬虫

从 `registry.yaml` 中删除或注释掉该 spider 条目，`git push`。sync flow 下次执行时会自动将其暂停。

### 永久删除某个爬虫

同上操作暂停后，可在 Prefect UI 中手动删除该 deployment（sync flow 不会自动删除，只会暂停）。

### 修改 sync flow 本身的调度频率

编辑 `spiders/deploy_sync_registry.py` 中的 `interval` 参数，然后在容器内重新执行：
```bash
docker exec -e PREFECT_API_URL=http://prefect-server:4200/api \
  -w /app/spiders prefect_demo-crawler-worker-1 \
  python deploy_sync_registry.py
```

---

## Prefect Variables 说明

sync flow 运行时依赖以下两个 Prefect Variable（自动创建，无需手动配置）：

| Variable 名 | 内容 | 用途 |
|------------|------|------|
| `registry_last_sha` | 字符串，12 位 SHA256 | 记录上次同步的 registry.yaml 内容哈希 |
| `registry_deployment_hashes` | JSON 字符串，`{"deployment名": "hash"}` | 记录每个 deployment 的配置哈希 |

> 若需强制全量重新部署所有 spider（例如 work pool 重建后），可在 Prefect UI 的 Variables 页面删除这两个变量，再手动触发一次 sync flow。

---

## 安全边界

sync flow 通过标签实现安全隔离，**只管理带 `managed-by:sync` 标签的 deployment**，不会影响手动创建的 deployment。该标签由 sync_registry.py 在构建 deployment 列表时自动追加，无需在 registry.yaml 中手动填写。
