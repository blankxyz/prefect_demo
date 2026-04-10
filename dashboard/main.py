import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="爬虫大屏管控网关")

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")
RUNTIME_FLAGS_PATH = Path(os.getenv("RUNTIME_FLAGS_PATH", "/app/runtime/system_flags.json"))
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


def _generator_ready() -> dict:
    return {
        "enabled": bool(LOCAL_CODEX_HELPER_URL) or bool(AI_API_KEY and AI_MODEL),
        "project_root_exists": PROJECT_ROOT.exists(),
        "project_root": str(PROJECT_ROOT),
        "model": AI_MODEL or None,
        "manager_deployment": DEPLOYMENT_MANAGER_NAME,
        "provider": "local_codex" if LOCAL_CODEX_HELPER_URL else "remote_api",
        "helper_url": LOCAL_CODEX_HELPER_URL or None,
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


async def _generate_code_via_ai(payload: dict) -> dict:
    readiness = _generator_ready()
    if not readiness["enabled"]:
        raise HTTPException(status_code=503, detail="AI 生成功能未配置，请设置 LOCAL_CODEX_HELPER_URL 或 AI_API_KEY + AI_MODEL")
    if not readiness["project_root_exists"]:
        raise HTTPException(status_code=500, detail=f"项目目录不存在: {PROJECT_ROOT}")

    raw_slug = str(payload.get("module_slug") or "")
    module_slug = _slugify(raw_slug)
    if not module_slug:
        raise HTTPException(status_code=400, detail="module_slug 不能为空，且必须包含字母或数字")

    requirement = str(payload.get("requirement") or "").strip()
    if len(requirement) < 20:
        raise HTTPException(status_code=400, detail="需求描述过短，请至少提供 20 个字符")

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

    if LOCAL_CODEX_HELPER_URL:
        async with httpx.AsyncClient(timeout=AI_TIMEOUT) as client:
            response = await client.post(
                f"{LOCAL_CODEX_HELPER_URL}/generate",
                json={
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                },
            )
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"本地 Codex helper 调用失败: {response.text}")
        generated = response.json()
    else:
        request_body = {
            "model": AI_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        }

        async with httpx.AsyncClient(timeout=AI_TIMEOUT) as client:
            response = await client.post(
                f"{AI_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {AI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=request_body,
            )

        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"AI 接口调用失败: {response.text}")

        try:
            raw_text = response.json()["choices"][0]["message"]["content"]
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"AI 返回结构异常: {exc}") from exc
        generated = _extract_json_object(raw_text)

    spider_code = str(generated.get("spider_code") or "").strip()
    deploy_code = str(generated.get("deploy_code") or "").strip()
    if not spider_code or not deploy_code:
        raise HTTPException(status_code=502, detail="AI 未返回完整代码")

    return {
        "module_slug": module_slug,
        "summary": str(generated.get("summary") or "").strip(),
        "spider_path": spider_path,
        "deploy_path": deploy_path,
        "flow_var": flow_var,
        "deployment_name": deployment_name,
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


@app.post("/api/generator/generate")
async def generate_spider_code(payload: dict):
    return await _generate_code_via_ai(payload)


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


app.mount("/", StaticFiles(directory="static", html=True), name="static")
