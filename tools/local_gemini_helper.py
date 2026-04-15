from __future__ import annotations

import json
import os
import re
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HOST = os.getenv("LOCAL_GEMINI_HELPER_HOST", "0.0.0.0")
PORT = int(os.getenv("LOCAL_GEMINI_HELPER_PORT", "8790"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/blank/playground/prefect_demo")).resolve()
GEMINI_BIN = os.getenv("GEMINI_BIN", "gemini")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "").strip()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
    handler.end_headers()
    handler.wfile.write(body)


def _extract_json(raw: str) -> dict:
    text = raw.strip()
    # 剥离可能的 Markdown 代码块
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise RuntimeError(f"Gemini 返回无法解析为 JSON，原始输出前 500 字:\n{raw[:500]}")
        return json.loads(match.group(0))


def _run_gemini(system_prompt: str, user_prompt: str) -> dict:
    combined_prompt = (
        "请严格只输出一个 JSON 对象，字段为 summary、spider_code、deploy_code，禁止任何 Markdown 代码块或额外文字。\n\n"
        "### System Instructions\n"
        f"{system_prompt}\n\n"
        "### User Request\n"
        f"{user_prompt}\n"
    )

    cmd = [
        GEMINI_BIN,
        "--yolo",
        "--output-format", "text",
        "--prompt", combined_prompt,
    ]
    if GEMINI_MODEL:
        cmd.extend(["--model", GEMINI_MODEL])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gemini 执行失败")

    return _extract_json(result.stdout)


class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
        _json_response(self, 200, {"ok": True})

    def do_GET(self) -> None:
        if self.path != "/health":
            _json_response(self, 404, {"error": "not_found"})
            return
        _json_response(
            self,
            200,
            {
                "status": "ok",
                "project_root": str(PROJECT_ROOT),
                "gemini_bin": GEMINI_BIN,
                "model": GEMINI_MODEL or None,
            },
        )

    def do_POST(self) -> None:
        if self.path != "/generate":
            _json_response(self, 404, {"error": "not_found"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            _json_response(self, 400, {"error": "invalid_json"})
            return

        system_prompt = str(payload.get("system_prompt") or "").strip()
        user_prompt = str(payload.get("user_prompt") or "").strip()
        if not system_prompt or not user_prompt:
            _json_response(self, 400, {"error": "missing_prompt"})
            return

        try:
            data = _run_gemini(system_prompt, user_prompt)
            _json_response(self, 200, data)
        except Exception as exc:
            _json_response(self, 500, {"error": str(exc)})

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"local gemini helper listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()
