# 生成器增强：SSE 流式输出 + 对话式迭代 + 预览沙箱

**日期：** 2026-04-13  
**状态：** 已审批

## 背景

当前生成阶段是一个阻塞式 HTTP 请求，用户看不到任何进度，不知道 Codex 是否还在工作。生成完毕后也没有办法在部署前验证 spider 实际能抓到什么数据，也缺乏对生成结果进行迭代修改的能力，导致可能把有问题的代码直接部署进生产。

目标：
1. 生成阶段改为 SSE 流式，实时推送 Codex 的思考过程
2. 生成后支持对话式迭代修改 spider_code，无需重新填写需求
3. 生成后提供"预览数据"功能，在 Prefect worker 沙箱中运行 spider 并返回前 5 条结果

---

## Feature 1：SSE 流式生成

### 改造链路

```
generator.html → /api/generator/generate-stream → local_codex_helper /generate-stream → codex subprocess
   EventSource        StreamingResponse (SSE)         Popen 流式读 stdout
```

### SSE 事件格式

| event | data 字段 | 说明 |
|-------|-----------|------|
| `progress` | `{message, elapsed}` | Codex stdout 逐行进度 |
| `result` | `{spider_code, deploy_code, summary, spider_path, deploy_path, ...}` | 生成完成 |
| `error` | `{message}` | 生成失败 |

### 涉及文件

**`tools/local_codex_helper.py`** — 新增 `/generate-stream` 端点：
- 用 `subprocess.Popen` 替换 `subprocess.run`
- 逐行读 `stdout`，每行作为 `progress` 事件写入响应
- 进程结束后读 `output_path` 文件，发送 `result` 事件
- 异常时发送 `error` 事件
- 响应头：`Content-Type: text/event-stream`，不设 `Content-Length`

**`dashboard/main.py`** — 新增 `POST /api/generator/generate-stream`：
- JSON payload：`{module_slug, requirement, helper_id}`（helper_id 可选）
- 用 `httpx.AsyncClient.stream("POST", ...)` 异步代理 helper 的 SSE 流
- 返回 `StreamingResponse(media_type="text/event-stream")`
- 原 `POST /api/generator/generate` 保持不变

**`dashboard/static/generator.html`** — 替换生成逻辑：
- 用 `fetch` + `response.body.getReader()` 替换原 `fetch` 调用
- `progress` 事件：更新状态栏（显示消息 + 已用时间）
- `result` 事件：填充代码编辑框，启用部署和预览按钮
- `error` 事件：显示错误状态

---

## Feature 2：对话式迭代修改

### 触发条件

第一轮生成成功后（`result` 事件到达），代码编辑框下方出现对话区域。

### 交互流程

```
[spider_code 编辑框]
[deploy_code 编辑框]（锁定，不参与迭代）

对话历史区：
  🤖 已生成 nrta_notice_spider.py，共 82 行。
  👤 URL 解析有问题，应该用 CSS 选择器而不是 XPath
  🤖 已根据反馈重新生成，改用 CSS 选择器...

[输入框：描述问题或调整需求...]  [重新生成]
```

### 前端状态

- `conversationHistory: []`：存储 `{role: "user"|"assistant", content: string}` 数组，仅存于 JS
- 每轮迭代把 `conversationHistory + 当前 spider_code + 新反馈` 发给后端
- AI 返回新 spider_code 后，更新编辑框并追加一条 assistant 消息到历史

### 后端变更

**`POST /api/generator/refine`** payload：
```json
{
  "module_slug": "nrta_notice",
  "spider_code": "当前编辑框内容",
  "feedback": "URL 解析有问题，改用 CSS 选择器",
  "history": [{"role": "user", "content": "..."}, ...]
}
```
- 构建专用 system prompt：只要求输出 `{summary, spider_code}`，不生成 deploy_code
- user prompt：原始代码 + 对话历史 + 新反馈
- 同样走 SSE 流式（复用 `/generate-stream` 链路）
- 返回事件：`progress` / `result`（只含 `spider_code` 和 `summary`）/ `error`

### 涉及文件（新增）

- `dashboard/main.py`：新增 `POST /api/generator/refine-stream`
- `tools/local_codex_helper.py`：`/generate-stream` 已支持，`refine` 复用同一端点，system prompt 由调用方控制
- `dashboard/static/generator.html`：新增对话区 HTML + `refineSpider()` 函数

---

## Feature 3：预览沙箱

### 整体流程

```
[预览数据] 按钮
    ↓
POST /api/generator/preview          写 spider 代码到磁盘，触发 preview_runner flow，返回 run_id
    ↓ 前端轮询（每 2s）
GET /api/generator/preview/{run_id}  查 Prefect flow run 状态；完成后读 preview_result.json
    ↓
展示结果表格（前 5 条，字段展开）
```

### 新增文件

**`spiders/preview_runner.py`**：
- Prefect flow，参数：`module_slug: str`
- monkey-patch 4 个函数（**必须在 `importlib.import_module` 之前**，利用 `from X import Y` 绑定语义）：
  - `common.result_sink.save_items_to_sinks` → 捕获数据到 `captured: list`
  - `common.clickhouse_sink.filter_new_items_by_url` → 跳过去重，直接返回原数据
  - `common.clickhouse_sink.save_data` → 空操作
  - `common.clickhouse_sink.insert_rows` → 空操作
- 动态 `importlib.import_module(f"spiders.{module_slug}_spider")` 并调用其 flow
- 通过 `asyncio.wait_for` 设 30 秒超时
- 取前 5 条写入 `PROJECT_ROOT/runtime/preview_{run_id}.json`：`{"module_slug": ..., "results": [...], "count": ...}`（用 run_id 避免并发覆盖）

**`spiders/deploy_preview_runner.py`**：
- 注册 `preview-runner` deployment 到 `docker-crawler-pool`
- 无调度（仅按需触发）

### 新增 API

**`POST /api/generator/preview`** payload：`{module_slug, spider_code}`  
- `_safe_relative_spider_path` 校验路径
- 写 spider 代码到 `spiders/{module_slug}_spider.py`
- 查找 `preview-runner` deployment ID，触发 flow run（参数：`module_slug`）
- 返回 `{run_id}`

**`GET /api/generator/preview/{run_id}`**  
- 查 Prefect `/flow_runs/{run_id}` 状态
- 状态为 `COMPLETED`：读 `runtime/preview_{run_id}.json`，返回 `{status: "done", results: [...]}`
- 状态为 `FAILED/CRASHED`：返回 `{status: "error", message: ...}`
- 其他：返回 `{status: "running"}`

### UI 变更

- 代码预览区下方新增"预览数据"按钮（初始禁用，生成成功后启用）
- 点击后显示 spinner + "运行中..."，轮询状态
- 完成后渲染结果表格：每行一条记录，列为字段名，值截断至 80 字符

---

## 涉及文件汇总

| 文件 | 操作 |
|------|------|
| `tools/local_codex_helper.py` | 修改（新增 `/generate-stream`） |
| `dashboard/main.py` | 修改（新增 SSE 端点 + refine 端点 + preview API） |
| `dashboard/static/generator.html` | 修改（SSE 接收 + 对话区 + 预览 UI） |
| `spiders/preview_runner.py` | 新建 |
| `spiders/deploy_preview_runner.py` | 新建 |
| `runtime/ai_helpers.json` | 修改（移除 Gemini helper） |

## 验证

1. **SSE**：点"生成代码"，状态栏实时滚动 Codex 输出，生成完毕后代码填入编辑框
2. **迭代**：在对话框输入反馈，点"重新生成"，spider_code 更新，deploy_code 不变，历史可见
3. **预览**：在 worker 先部署 `deploy_preview_runner.py`，点"预览数据"，约 30s 内看到结果表格
4. 结果表格内容与目标站点实际数据一致，原部署流程不受影响
