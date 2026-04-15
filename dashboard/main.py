import json
import os
import re
import subprocess
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="爬虫大屏管控网关")

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
RUNTIME_FLAGS_PATH = Path(os.getenv("RUNTIME_FLAGS_PATH", "/app/runtime/system_flags.json"))
AI_HELPERS_PATH = Path(os.getenv("RUNTIME_FLAGS_PATH", "/app/runtime/system_flags.json")).parent / "ai_helpers.json"
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/workspace"))
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_MODEL = os.getenv("AI_MODEL", "").strip()
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT", "180"))
DEPLOYMENT_MANAGER_NAME = os.getenv("DEPLOYMENT_MANAGER_NAME", "platform-register-deployment")
LOCAL_CODEX_HELPER_URL = os.getenv("LOCAL_CODEX_HELPER_URL", "").strip().rstrip("/")
MAX_CONTEXT_CHARS = int(os.getenv("GENERATOR_MAX_CONTEXT_CHARS", "24000"))
REFERENCE_FILES: tuple[tuple[str, int], ...] = (
    ("common/nrta_base.py", 7000),
    ("spiders/gdj_xydt_spider.py", 7000),
    ("common/result_sink.py", 3500),
    ("common/clickhouse_sink.py", 3500),
    ("spiders/deploy_nrta_activity.py", 2500),
)

GENERATOR_SYSTEM_PROMPT = """
你是一个资深 Python 爬虫工程师。你只为当前项目生成可直接落地的代码。

必须遵守：
1. 只输出一个 JSON 对象，禁止 Markdown 代码块。
2. JSON 字段仅允许：
   - summary: string
   - spider_code: string
   - deploy_code: string
3. spider_code 必须：
   - 使用 Python 3.11+ 语法
   - 定义名为 {flow_var} 的 Prefect flow
   - 文件路径固定为 spiders/{module_slug}_spider.py
   - 优先复用 common.spider_common、common.result_sink、common.clickhouse_sink、common.nrta_base
   - 输出统一数据结构字段：
     spidertime, content, publishtime, tbid, url, author, title, accountcode, spiderid, createtime, publishdate, type, browsenum
   - 默认只抓第一页
   - 默认先按 site_name + url 去重，再抓详情，再写 ClickHouse，再按运行时开关决定是否发 Kafka
   - 如果站点结构足够像 NRTA 的静态详情页，优先使用 common.nrta_base
4. deploy_code 必须：
   - 文件路径固定为 spiders/deploy_{module_slug}.py
   - 导入 spiders/{module_slug}_spider.py 中的 {flow_var}
   - 创建 deployment 名称 {deployment_name}
   - work_pool_name 固定为 docker-crawler-pool
   - 调度固定为每天一次
5. 不要依赖项目中不存在的模块。
6. 不要输出解释文本。
""".strip()


REFINE_SYSTEM_PROMPT = """
你是一个资深 Python 爬虫工程师。根据用户反馈，修改现有的 spider 代码。

必须遵守：
1. 只输出一个 JSON 对象，禁止 Markdown 代码块。
2. JSON 字段仅允许：
   - summary: string（说明本次修改了什么）
   - spider_code: string（完整的修改后代码）
3. 不要修改 deploy 文件，不要输出 deploy_code。
4. 不要输出解释文本。
""".strip()

REFINE_OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["summary", "spider_code"],
    "properties": {
        "summary": {"type": "string"},
        "spider_code": {"type": "string"},
    },
}

GEN_VALIDATE_MAX_ATTEMPTS = int(os.getenv("GEN_VALIDATE_MAX_ATTEMPTS", "3"))
GEN_VALIDATE_TIMEOUT_SECONDS = int(os.getenv("GEN_VALIDATE_TIMEOUT_SECONDS", "120"))
GEN_VALIDATE_POLL_SECONDS = float(os.getenv("GEN_VALIDATE_POLL_SECONDS", "2"))


def _default_flags() -> dict:
    return {"enable_kafka": True}


def _read_flags() -> dict:
    try:
        if not RUNTIME_FLAGS_PATH.exists():
            return _default_flags()
        with RUNTIME_FLAGS_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_flags()
        return {**_default_flags(), **data}
    except Exception:
        return _default_flags()


def _write_flags(flags: dict) -> None:
    RUNTIME_FLAGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RUNTIME_FLAGS_PATH.open("w", encoding="utf-8") as f:
        json.dump(flags, f, ensure_ascii=False, indent=2)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    return slug


def _read_ai_helpers() -> list[dict]:
    """读取 ai_helpers.json，不存在时从环境变量构建默认列表。"""
    if AI_HELPERS_PATH.exists():
        try:
            data = json.loads(AI_HELPERS_PATH.read_text(encoding="utf-8"))
            helpers = data.get("helpers") if isinstance(data, dict) else None
            if isinstance(helpers, list) and helpers:
                return helpers
        except Exception:
            pass

    # 回退：从环境变量构建
    fallback: list[dict] = []
    if LOCAL_CODEX_HELPER_URL:
        fallback.append({
            "id": "codex",
            "name": "本地 Codex Helper",
            "type": "codex",
            "url": LOCAL_CODEX_HELPER_URL,
        })
    if AI_API_KEY and AI_MODEL:
        fallback.append({
            "id": "remote_api",
            "name": f"远程 API ({AI_MODEL})",
            "type": "openai",
            "base_url": AI_BASE_URL,
            "api_key": AI_API_KEY,
            "model": AI_MODEL,
        })
    return fallback


def _generator_ready() -> dict:
    helpers = _read_ai_helpers()
    first = helpers[0] if helpers else {}
    return {
        "enabled": bool(helpers),
        "helper_count": len(helpers),
        "project_root_exists": PROJECT_ROOT.exists(),
        "project_root": str(PROJECT_ROOT),
        "model": first.get("model") or None,
        "manager_deployment": DEPLOYMENT_MANAGER_NAME,
        "provider": first.get("type") or None,
        "reference_files": [path for path, _ in REFERENCE_FILES],
    }


def _build_run_summary(flow_runs: list[dict], deployment_name_map: dict[str, str], flow_name_map: dict[str, str]) -> tuple[list[dict], dict]:
    now = datetime.now(timezone.utc)
    execution_runs: list[dict] = []

    for run in flow_runs:
        state_type = (run.get("state_type") or "").upper()
        start_time = _parse_dt(run.get("start_time"))
        created_time = _parse_dt(run.get("created"))
        end_time = _parse_dt(run.get("end_time"))

        is_execution = bool(start_time) or state_type in {"RUNNING", "COMPLETED", "FAILED", "CRASHED", "CANCELLED", "PENDING"}
        if not is_execution:
            continue

        display_name = (
            deployment_name_map.get(run.get("deployment_id"))
            or flow_name_map.get(run.get("flow_id"))
            or run.get("name")
        )
        trigger_type = ((run.get("created_by") or {}).get("type") or "").upper()
        total_run_time = run.get("total_run_time") or 0
        if not total_run_time and start_time:
            if state_type == "RUNNING":
                total_run_time = max(0.0, (now - start_time).total_seconds())
            elif end_time:
                total_run_time = max(0.0, (end_time - start_time).total_seconds())

        execution_runs.append(
            {
                **run,
                "display_name": display_name,
                "trigger_label": "调度" if trigger_type == "SCHEDULE" else "手动",
                "display_time": (start_time or created_time or end_time or now).astimezone(timezone.utc).isoformat(),
                "duration_seconds": round(float(total_run_time), 1),
            }
        )

    execution_runs.sort(
        key=lambda run: _parse_dt(run.get("start_time")) or _parse_dt(run.get("created")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    recent_24h = [run for run in execution_runs if (_parse_dt(run.get("start_time")) or _parse_dt(run.get("created")) or now) >= now - timedelta(hours=24)]
    summary = {
        "deployment_count": len(deployment_name_map),
        "running_count": sum(1 for run in execution_runs if (run.get("state_type") or "").upper() == "RUNNING"),
        "success_24h": sum(1 for run in recent_24h if (run.get("state_type") or "").upper() == "COMPLETED"),
        "failed_24h": sum(1 for run in recent_24h if (run.get("state_type") or "").upper() in {"FAILED", "CRASHED"}),
    }
    return execution_runs[:12], summary


def _extract_json_object(raw_text: str) -> dict:
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n", "", text)
        text = re.sub(r"\n```$", "", text)

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise HTTPException(status_code=502, detail="模型返回无法解析为 JSON")
        data = json.loads(match.group(0))

    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="模型返回格式错误")
    return data


def _safe_relative_spider_path(path_value: str) -> Path:
    relative = Path(path_value)
    if relative.is_absolute():
        raise HTTPException(status_code=400, detail="文件路径必须是相对路径")
    resolved = (PROJECT_ROOT / relative).resolve()
    project_root_resolved = PROJECT_ROOT.resolve()
    if project_root_resolved not in resolved.parents:
        raise HTTPException(status_code=400, detail="文件路径超出项目目录")
    if "spiders" not in resolved.parts:
        raise HTTPException(status_code=400, detail="当前仅允许写入 spiders 目录")
    return resolved


def _deploy_script_from_deployment_name(deployment_name: str) -> str:
    normalized = (deployment_name or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="deployment_name 不能为空")

    if normalized == DEPLOYMENT_MANAGER_NAME:
        script_name = "deploy_platform_ops.py"
    else:
        base_name = normalized[:-5] if normalized.endswith("-flow") else normalized
        base_name = base_name.replace("-", "_")
        script_name = f"deploy_{base_name}.py"

    relative = Path("spiders") / script_name
    resolved = (PROJECT_ROOT / relative).resolve()
    if not resolved.is_file():
        raise HTTPException(status_code=404, detail=f"未找到部署脚本: {relative.as_posix()}")
    return relative.as_posix()


def _trim_reference_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = text[: max_chars // 2]
    tail = text[-(max_chars // 2) :]
    return f"{head}\n\n# ... 中间内容省略 ...\n\n{tail}"


def _build_project_context() -> str:
    blocks: list[str] = []
    total_chars = 0

    for relative_path, per_file_limit in REFERENCE_FILES:
        full_path = (PROJECT_ROOT / relative_path).resolve()
        if not full_path.is_file():
            continue
        raw = full_path.read_text(encoding="utf-8")
        excerpt = _trim_reference_text(raw, per_file_limit)
        block = f"### FILE: {relative_path}\n```python\n{excerpt}\n```"
        if total_chars + len(block) > MAX_CONTEXT_CHARS:
            break
        blocks.append(block)
        total_chars += len(block)

    rules = """
### PROJECT RULES
- 所有结果都必须复用当前项目已有公共模块，优先避免重复造轮子。
- 统一输出字段：spidertime, content, publishtime, tbid, url, author, title, accountcode, spiderid, createtime, publishdate, type, browsenum
- 默认只抓第一页。
- 默认先按 site_name + url 去重，再抓详情，再写 ClickHouse，再根据运行时开关决定是否发 Kafka。
- deployment 通过 deploy_*.py 注册到 Prefect，执行侧在 worker，不在 dashboard。
- 能走 common.nrta_base 的，优先走配置化实现。
- 不能走 nrta_base 的，再写独立 spider，但仍要复用 common.result_sink 与 common.clickhouse_sink。
""".strip()

    return f"{rules}\n\n" + "\n\n".join(blocks)


async def _find_deployment_id_by_name(client: httpx.AsyncClient, deployment_name: str) -> str | None:
    resp = await client.post(f"{PREFECT_API_URL}/deployments/filter", json={})
    if resp.status_code != 200:
        return None
    for dep in resp.json():
        if dep.get("name") == deployment_name:
            return dep.get("id")
    return None


async def _start_preview_flow_run(
    client: httpx.AsyncClient,
    *,
    module_slug: str,
    spider_code: str,
) -> str:
    spider_file_path = _safe_relative_spider_path(f"spiders/{module_slug}_spider.py")
    spider_file_path.parent.mkdir(parents=True, exist_ok=True)
    spider_file_path.write_text(spider_code, encoding="utf-8")

    preview_dep_id = await _find_deployment_id_by_name(client, "preview-runner")
    if not preview_dep_id:
        raise HTTPException(
            status_code=503,
            detail="未找到 preview-runner deployment，请先在 worker 内执行 python spiders/deploy_preview_runner.py",
        )

    resp = await client.post(
        f"{PREFECT_API_URL}/deployments/{preview_dep_id}/create_flow_run",
        json={"parameters": {"module_slug": module_slug}},
    )
    if resp.status_code not in {200, 201}:
        raise HTTPException(status_code=502, detail=f"触发预览失败: {resp.text}")
    run_id = resp.json().get("id")
    if not run_id:
        raise HTTPException(status_code=502, detail="预览任务未返回 run_id")
    return run_id


async def _read_preview_flow_run(client: httpx.AsyncClient, run_id: str) -> dict:
    resp = await client.get(f"{PREFECT_API_URL}/flow_runs/{run_id}")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"查询 flow run 失败: {resp.text}")

    run_data = resp.json()
    state_type = (run_data.get("state_type") or "").upper()

    if state_type == "COMPLETED":
        result_path = PROJECT_ROOT / "runtime" / f"preview_{run_id}.json"
        if not result_path.exists():
            return {"status": "done", "results": [], "count": 0, "warning": "结果文件不存在"}
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return {"status": "error", "message": f"读取结果文件失败: {exc}"}
        return {"status": "done", **result}
    if state_type in {"FAILED", "CRASHED", "CANCELLED"}:
        message = (run_data.get("state") or {}).get("message") or state_type
        return {"status": "error", "message": message}
    return {"status": "running", "state": state_type}


async def _wait_preview_result(
    client: httpx.AsyncClient,
    run_id: str,
    *,
    timeout_seconds: int = GEN_VALIDATE_TIMEOUT_SECONDS,
    poll_seconds: float = GEN_VALIDATE_POLL_SECONDS,
) -> dict:
    start = datetime.now(timezone.utc)
    while True:
        result = await _read_preview_flow_run(client, run_id)
        if result.get("status") in {"done", "error"}:
            return result
        if (datetime.now(timezone.utc) - start).total_seconds() > timeout_seconds:
            return {"status": "error", "message": f"预览轮询超时（>{timeout_seconds}s）"}
        await asyncio.sleep(poll_seconds)


async def _call_codex_helper(helper: dict, system_prompt: str, user_prompt: str) -> dict:
    url = str(helper.get("url") or "").rstrip("/")
    if not url:
        raise HTTPException(status_code=503, detail=f"Helper '{helper.get('name')}' 未配置 url")
    # 本地 helper 需要较长的 read timeout（CLI 模型推理可能耗时数分钟）
    timeout = httpx.Timeout(connect=10.0, read=600.0, write=30.0, pool=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            f"{url}/generate",
            json={"system_prompt": system_prompt, "user_prompt": user_prompt},
        )
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Codex helper 调用失败: {response.text}")
    return response.json()


async def _call_openai_helper(helper: dict, system_prompt: str, user_prompt: str) -> dict:
    base_url = str(helper.get("base_url") or "").rstrip("/")
    api_key = str(helper.get("api_key") or "")
    model = str(helper.get("model") or "")
    if not base_url or not model:
        raise HTTPException(status_code=503, detail=f"Helper '{helper.get('name')}' 未配置 base_url 或 model")

    request_body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    async with httpx.AsyncClient(timeout=AI_TIMEOUT) as client:
        response = await client.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json=request_body,
        )

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"OpenAI 接口调用失败: {response.text}")

    try:
        raw_text = response.json()["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI 返回结构异常: {exc}") from exc
    return _extract_json_object(raw_text)


def _build_codegen_prompts(module_slug: str, requirement: str) -> tuple[str, str, dict]:
    """构建代码生成的 system_prompt、user_prompt 和元数据。"""
    flow_var = f"{module_slug}_flow"
    deployment_name = f"{module_slug}-flow"
    spider_path = f"spiders/{module_slug}_spider.py"
    deploy_path = f"spiders/deploy_{module_slug}.py"

    user_prompt = f"""
模块代号: {module_slug}
Spider 文件: {spider_path}
Deploy 文件: {deploy_path}
Flow 变量名: {flow_var}
Deployment 名称: {deployment_name}

项目内可参考模式：
- NRTA 配置式实现：common/nrta_base.py
- 辽宁独立实现：spiders/gdj_xydt_spider.py
- 部署脚本样式：spiders/deploy_nrta_activity.py

用户需求如下：
{requirement}

请结合下面的项目上下文与参考实现生成代码：
{_build_project_context()}
""".strip()

    system_prompt = GENERATOR_SYSTEM_PROMPT.format(
        flow_var=flow_var,
        module_slug=module_slug,
        deployment_name=deployment_name,
    )

    meta = {
        "module_slug": module_slug,
        "spider_path": spider_path,
        "deploy_path": deploy_path,
        "flow_var": flow_var,
        "deployment_name": deployment_name,
    }
    return system_prompt, user_prompt, meta


async def _proxy_codex_sse(
    helper: dict,
    system_prompt: str,
    user_prompt: str,
    output_schema: dict | None,
    inject_fields: dict | None = None,
) -> StreamingResponse:
    """代理 codex helper 的 SSE 流，可选在 result 事件中注入额外字段。"""
    url = str(helper.get("url") or "").rstrip("/")
    if not url:
        raise HTTPException(status_code=503, detail=f"Helper '{helper.get('name')}' 未配置 url")

    request_body: dict = {"system_prompt": system_prompt, "user_prompt": user_prompt}
    if output_schema:
        request_body["output_schema"] = output_schema

    async def _event_generator() -> AsyncIterator[str]:
        timeout = httpx.Timeout(connect=10.0, read=600.0, write=30.0, pool=5.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("POST", f"{url}/generate-stream", json=request_body) as response:
                    buffer = ""
                    async for chunk in response.aiter_text():
                        buffer += chunk
                        while "\n\n" in buffer:
                            block, buffer = buffer.split("\n\n", 1)
                            block = block.strip()
                            if not block:
                                continue
                            event_type: str | None = None
                            data_str: str | None = None
                            for line in block.split("\n"):
                                if line.startswith("event:"):
                                    event_type = line[6:].strip()
                                elif line.startswith("data:"):
                                    data_str = line[5:].strip()
                            if event_type == "result" and data_str and inject_fields:
                                try:
                                    data = json.loads(data_str)
                                    data.update(inject_fields)
                                    yield f"event: result\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
                                    continue
                                except Exception:
                                    pass
                            yield f"{block}\n\n"
        except Exception as exc:
            yield f"event: error\ndata: {json.dumps({'message': str(exc)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_event_generator(), media_type="text/event-stream")


async def _generate_code_via_helper(helper: dict, payload: dict) -> dict:
    if not PROJECT_ROOT.exists():
        raise HTTPException(status_code=500, detail=f"项目目录不存在: {PROJECT_ROOT}")

    raw_slug = str(payload.get("module_slug") or "")
    module_slug = _slugify(raw_slug)
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 不能为空，且必须包含字母或数字")

    requirement = str(payload.get("requirement") or "").strip()
    if len(requirement) < 20:
        raise HTTPException(status_code=400, detail="需求描述过短，请至少提供 20 个字符")

    system_prompt, user_prompt, meta = _build_codegen_prompts(module_slug, requirement)

    helper_type = str(helper.get("type") or "")
    if helper_type == "codex":
        generated = await _call_codex_helper(helper, system_prompt, user_prompt)
    elif helper_type == "openai":
        generated = await _call_openai_helper(helper, system_prompt, user_prompt)
    else:
        raise HTTPException(status_code=400, detail=f"未知 helper type: {helper_type}")

    spider_code = str(generated.get("spider_code") or "").strip()
    deploy_code = str(generated.get("deploy_code") or "").strip()
    if not spider_code or not deploy_code:
        raise HTTPException(status_code=502, detail="AI 未返回完整代码")

    return {
        **meta,
        "summary": str(generated.get("summary") or "").strip(),
        "spider_code": spider_code + "\n",
        "deploy_code": deploy_code + "\n",
    }


def _syntax_check(paths: list[Path]) -> None:
    cmd = [sys.executable, "-m", "py_compile", *[str(path) for path in paths]]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        raise HTTPException(
            status_code=400,
            detail=f"生成代码语法检查失败:\n{result.stderr or result.stdout}",
        )


@app.get("/api/dashboard_state")
async def get_dashboard_state():
    async with httpx.AsyncClient() as client:
        try:
            deployments_resp = await client.post(f"{PREFECT_API_URL}/deployments/filter", json={})
            deployments = deployments_resp.json() if deployments_resp.status_code == 200 else []

            flows_resp = await client.post(f"{PREFECT_API_URL}/flows/filter", json={})
            flows = flows_resp.json() if flows_resp.status_code == 200 else []
            flow_name_map = {
                flow.get("id"): flow.get("name")
                for flow in flows
                if isinstance(flow, dict)
            }

            deployment_name_map = {}
            for dep in deployments:
                flow_name = flow_name_map.get(dep.get("flow_id"))
                display_name = flow_name or dep.get("description") or dep.get("name")
                dep["flow_name"] = flow_name
                dep["display_name"] = display_name
                deployment_name_map[dep.get("id")] = display_name

            flow_runs_resp = await client.post(
                f"{PREFECT_API_URL}/flow_runs/filter",
                json={"sort": "START_TIME_DESC", "limit": 60},
            )
            flow_runs = flow_runs_resp.json() if flow_runs_resp.status_code == 200 else []
            execution_runs, run_summary = _build_run_summary(flow_runs, deployment_name_map, flow_name_map)

            return {
                "deployments": deployments,
                "flow_runs": execution_runs,
                "run_summary": run_summary,
                "api_health": "UP",
                "settings": _read_flags(),
                "generator": _generator_ready(),
            }
        except Exception as e:
            return {
                "error": str(e),
                "api_health": "DOWN",
                "settings": _read_flags(),
                "run_summary": {},
                "generator": _generator_ready(),
            }


@app.post("/api/trigger/{deployment_id}")
async def trigger_spider(deployment_id: str):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{PREFECT_API_URL}/deployments/{deployment_id}/create_flow_run", json={})
            if resp.status_code in [200, 201]:
                return {"message": "爬虫下发执行成功！", "data": resp.json()}
            raise HTTPException(status_code=400, detail=resp.text)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/settings")
async def get_settings():
    return _read_flags()


@app.post("/api/settings/kafka")
async def update_kafka_setting(payload: dict):
    if "enable_kafka" not in payload:
        raise HTTPException(status_code=400, detail="缺少 enable_kafka 字段")

    flags = _read_flags()
    flags["enable_kafka"] = bool(payload["enable_kafka"])
    _write_flags(flags)
    return {"message": "Kafka 开关已更新", "settings": flags}


@app.get("/api/generator/helpers")
async def list_ai_helpers():
    helpers = _read_ai_helpers()
    safe = []
    for h in helpers:
        entry = {k: v for k, v in h.items() if k != "api_key"}
        if "api_key" in h:
            entry["api_key_set"] = bool(h["api_key"])
        safe.append(entry)
    return {"helpers": safe}


@app.post("/api/generator/generate")
async def generate_spider_code(payload: dict):
    helpers = _read_ai_helpers()
    if not helpers:
        raise HTTPException(status_code=503, detail="AI 生成功能未配置，请检查 runtime/ai_helpers.json 或环境变量")

    helper_id = str(payload.get("helper_id") or "").strip()
    if helper_id:
        helper = next((h for h in helpers if h.get("id") == helper_id), None)
        if not helper:
            raise HTTPException(status_code=400, detail=f"未找到 helper_id: {helper_id}")
    else:
        helper = helpers[0]

    return await _generate_code_via_helper(helper, payload)


@app.post("/api/generator/generate-validated")
async def generate_spider_code_with_validation(payload: dict):
    helpers = _read_ai_helpers()
    if not helpers:
        raise HTTPException(status_code=503, detail="AI 生成功能未配置，请检查 runtime/ai_helpers.json 或环境变量")

    helper_id = str(payload.get("helper_id") or "").strip()
    if helper_id:
        helper = next((h for h in helpers if h.get("id") == helper_id), None)
        if not helper:
            raise HTTPException(status_code=400, detail=f"未找到 helper_id: {helper_id}")
    else:
        helper = helpers[0]

    max_attempts = int(payload.get("max_attempts") or GEN_VALIDATE_MAX_ATTEMPTS)
    if max_attempts < 1:
        max_attempts = 1
    if max_attempts > 5:
        max_attempts = 5

    module_slug = _slugify(str(payload.get("module_slug") or ""))
    requirement = str(payload.get("requirement") or "").strip()
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 不能为空，且必须包含字母或数字")
    if len(requirement) < 20:
        raise HTTPException(status_code=400, detail="需求描述过短，请至少提供 20 个字符")

    last_generated: dict | None = None
    last_preview: dict | None = None
    feedback: str | None = None

    async with httpx.AsyncClient() as client:
        for attempt in range(1, max_attempts + 1):
            effective_requirement = requirement
            if feedback:
                effective_requirement = (
                    f"{requirement}\n\n"
                    f"【上一次自动验证失败反馈】\n{feedback}\n"
                    "请据此修正抓取规则，确保预览能抓到正文数据。"
                )

            generated = await _generate_code_via_helper(
                helper,
                {"module_slug": module_slug, "requirement": effective_requirement},
            )
            last_generated = generated

            run_id = await _start_preview_flow_run(
                client,
                module_slug=module_slug,
                spider_code=generated["spider_code"],
            )
            preview_result = await _wait_preview_result(
                client,
                run_id,
                timeout_seconds=GEN_VALIDATE_TIMEOUT_SECONDS,
                poll_seconds=GEN_VALIDATE_POLL_SECONDS,
            )
            last_preview = {"run_id": run_id, **preview_result}

            if preview_result.get("status") == "done" and int(preview_result.get("count") or 0) > 0:
                return {
                    **generated,
                    "validated": True,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "preview": last_preview,
                }

            if preview_result.get("status") == "done":
                feedback = "预览抓取结果为 0 条。"
            else:
                feedback = str(preview_result.get("message") or "预览运行失败")

    if not last_generated:
        raise HTTPException(status_code=502, detail="生成失败，未产出代码")

    return {
        **last_generated,
        "validated": False,
        "attempt": max_attempts,
        "max_attempts": max_attempts,
        "preview": last_preview or {"status": "error", "message": "未获得预览结果"},
    }


@app.post("/api/generator/deploy")
async def save_and_request_deploy(payload: dict):
    module_slug = _slugify(str(payload.get("module_slug") or ""))
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 无效")

    spider_path = _safe_relative_spider_path(str(payload.get("spider_path") or ""))
    deploy_path = _safe_relative_spider_path(str(payload.get("deploy_path") or ""))
    spider_code = str(payload.get("spider_code") or "")
    deploy_code = str(payload.get("deploy_code") or "")
    if not spider_code.strip() or not deploy_code.strip():
        raise HTTPException(status_code=400, detail="缺少 spider_code 或 deploy_code")

    spider_path.parent.mkdir(parents=True, exist_ok=True)
    spider_path.write_text(spider_code, encoding="utf-8")
    deploy_path.write_text(deploy_code, encoding="utf-8")
    _syntax_check([spider_path, deploy_path])

    deploy_script = str(deploy_path.relative_to(PROJECT_ROOT)).replace("\\", "/")

    async with httpx.AsyncClient() as client:
        manager_id = await _find_deployment_id_by_name(client, DEPLOYMENT_MANAGER_NAME)
        if not manager_id:
            raise HTTPException(
                status_code=503,
                detail=f"未找到 worker 侧部署注册器: {DEPLOYMENT_MANAGER_NAME}",
            )

        resp = await client.post(
            f"{PREFECT_API_URL}/deployments/{manager_id}/create_flow_run",
            json={"parameters": {"deploy_script": deploy_script}},
        )
        if resp.status_code not in {200, 201}:
            raise HTTPException(status_code=502, detail=f"提交 worker 部署任务失败: {resp.text}")

    return {
        "message": "代码已写入，已提交 worker 注册部署任务",
        "module_slug": module_slug,
        "deploy_script": deploy_script,
        "flow_run": resp.json(),
    }


@app.post("/api/redeploy/{deployment_name}")
async def redeploy_existing(deployment_name: str):
    deploy_script = _deploy_script_from_deployment_name(deployment_name)

    async with httpx.AsyncClient() as client:
        manager_id = await _find_deployment_id_by_name(client, DEPLOYMENT_MANAGER_NAME)
        if not manager_id:
            raise HTTPException(
                status_code=503,
                detail=f"未找到 worker 侧部署注册器: {DEPLOYMENT_MANAGER_NAME}",
            )

        resp = await client.post(
            f"{PREFECT_API_URL}/deployments/{manager_id}/create_flow_run",
            json={"parameters": {"deploy_script": deploy_script}},
        )
        if resp.status_code not in {200, 201}:
            raise HTTPException(status_code=502, detail=f"提交 worker 部署任务失败: {resp.text}")

    return {
        "message": "已提交重新部署任务",
        "deployment_name": deployment_name,
        "deploy_script": deploy_script,
        "flow_run": resp.json(),
    }


@app.post("/api/generator/generate-stream")
async def generate_spider_stream(payload: dict):
    helpers = _read_ai_helpers()
    if not helpers:
        raise HTTPException(status_code=503, detail="AI 生成功能未配置，请检查 runtime/ai_helpers.json 或环境变量")

    helper_id = str(payload.get("helper_id") or "").strip()
    if helper_id:
        helper = next((h for h in helpers if h.get("id") == helper_id), helpers[0])
    else:
        helper = helpers[0]

    if str(helper.get("type") or "") != "codex":
        raise HTTPException(status_code=400, detail="SSE 流式生成仅支持 codex 类型 helper，请使用 /api/generator/generate")

    raw_slug = str(payload.get("module_slug") or "")
    module_slug = _slugify(raw_slug)
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 不能为空，且必须包含字母或数字")

    requirement = str(payload.get("requirement") or "").strip()
    if len(requirement) < 20:
        raise HTTPException(status_code=400, detail="需求描述过短，请至少提供 20 个字符")

    system_prompt, user_prompt, inject_fields = _build_codegen_prompts(module_slug, requirement)
    return await _proxy_codex_sse(helper, system_prompt, user_prompt, None, inject_fields)


@app.post("/api/generator/refine-stream")
async def refine_spider_stream(payload: dict):
    helpers = _read_ai_helpers()
    if not helpers:
        raise HTTPException(status_code=503, detail="AI 生成功能未配置")

    helper_id = str(payload.get("helper_id") or "").strip()
    if helper_id:
        helper = next((h for h in helpers if h.get("id") == helper_id), helpers[0])
    else:
        helper = helpers[0]

    if str(helper.get("type") or "") != "codex":
        raise HTTPException(status_code=400, detail="SSE 流式生成仅支持 codex 类型 helper")

    spider_code = str(payload.get("spider_code") or "").strip()
    feedback = str(payload.get("feedback") or "").strip()
    if not spider_code or not feedback:
        raise HTTPException(status_code=400, detail="缺少 spider_code 或 feedback")

    history: list[dict] = payload.get("history") or []
    history_lines: list[str] = []
    for msg in history:
        role_label = "用户" if msg.get("role") == "user" else "AI"
        history_lines.append(f"{role_label}: {msg.get('content', '')}")
    history_block = ("\n历史对话：\n" + "\n".join(history_lines) + "\n") if history_lines else ""

    user_prompt = f"当前 spider 代码：\n```python\n{spider_code}\n```\n{history_block}\n用户新反馈：{feedback}"
    return await _proxy_codex_sse(helper, REFINE_SYSTEM_PROMPT, user_prompt, REFINE_OUTPUT_SCHEMA, None)


@app.post("/api/generator/preview")
async def start_preview(payload: dict):
    module_slug = _slugify(str(payload.get("module_slug") or ""))
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 无效")

    spider_code = str(payload.get("spider_code") or "").strip()
    if not spider_code:
        raise HTTPException(status_code=400, detail="缺少 spider_code")

    async with httpx.AsyncClient() as client:
        run_id = await _start_preview_flow_run(client, module_slug=module_slug, spider_code=spider_code)
    return {"run_id": run_id}


@app.get("/api/generator/preview/{run_id}")
async def poll_preview(run_id: str):
    async with httpx.AsyncClient() as client:
        return await _read_preview_flow_run(client, run_id)


app.mount("/", StaticFiles(directory="static", html=True), name="static")
