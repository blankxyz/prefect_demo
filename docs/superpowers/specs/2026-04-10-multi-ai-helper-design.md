# 多 AI Helper 切换设计

**日期：** 2026-04-10  
**状态：** 已审批

## 背景

当前 dashboard 的 AI 爬虫生成器只支持单一 AI 后端，通过环境变量在 Docker 启动时固定。当 token 耗尽时需要修改环境变量并重启容器才能切换，体验很差。

目标：支持预配置多个 AI Helper，在 generator 页面通过下拉框随时切换，无需重启 Docker。

## 设计

### 配置文件：`runtime/ai_helpers.json`

```json
{
  "helpers": [
    {
      "id": "codex",
      "name": "本地 Codex Helper",
      "type": "codex",
      "url": "http://host.docker.internal:8789"
    },
    {
      "id": "local_model",
      "name": "本地模型",
      "type": "openai",
      "base_url": "http://host.docker.internal:11434/v1",
      "api_key": "",
      "model": "qwen2.5-coder:14b"
    }
  ]
}
```

两种 `type`：
- `codex`：调用 `url/generate`，body 为 `{system_prompt, user_prompt}`，对应 `tools/local_codex_helper.py` 协议
- `openai`：调用 `base_url/chat/completions`，标准 OpenAI 格式，兼容 Ollama / LM Studio / 远程 API

**回退策略：** 文件不存在时，从现有环境变量（`LOCAL_CODEX_HELPER_URL` / `AI_API_KEY + AI_MODEL`）构建默认 helper 列表，保持向后兼容。

### 后端变更：`dashboard/main.py`

新增函数：
- `_read_ai_helpers() -> list[dict]`：读取配置文件，不存在则从环境变量构建
- `_generate_code_via_helper(helper: dict, payload: dict) -> dict`：根据 `helper["type"]` 分支调用，替换现有 `_generate_code_via_ai()`

新增 API：
- `GET /api/generator/helpers`：返回 helper 列表，**过滤掉 `api_key` 字段**

修改 API：
- `POST /api/generator/generate`：payload 增加可选 `helper_id: str`，未传时取列表第一个
- `_generator_ready()`：改为读配置文件，反映实际可用 helper 数量

### 前端变更：`dashboard/static/generator.html`

- 页面加载时调用 `GET /api/generator/helpers`，在表单顶部渲染 helper 下拉框
- 点击"生成代码"时，将当前选中的 `helper_id` 附带在请求 body 中
- 状态消息栏初始化时显示当前 helper 名称和模型信息

## 涉及文件

| 文件 | 操作 |
|------|------|
| `runtime/ai_helpers.json` | 新建（初始配置） |
| `dashboard/main.py` | 修改（新增函数和 API） |
| `dashboard/static/generator.html` | 修改（添加下拉框和联动逻辑） |

## 验证

1. 新建 `runtime/ai_helpers.json` 配置两个 helper
2. 重建 dashboard 容器：`docker compose build custom-dashboard && docker compose up -d custom-dashboard`
3. 访问 `http://localhost:8280/generator.html`，确认顶部显示 helper 下拉框
4. 切换到第二个 helper，点击"生成代码"，确认调用正确的后端
5. 删除 `ai_helpers.json`，确认回退到环境变量行为，功能不受影响
