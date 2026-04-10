from __future__ import annotations

import json
import os
import subprocess
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HOST = os.getenv("LOCAL_CODEX_HELPER_HOST", "0.0.0.0")
PORT = int(os.getenv("LOCAL_CODEX_HELPER_PORT", "8789"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/blank/playground/prefect_demo")).resolve()
CODEX_BIN = os.getenv("CODEX_BIN", "codex")
CODEX_MODEL = os.getenv("CODEX_MODEL", "").strip()


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


def _run_codex(system_prompt: str, user_prompt: str) -> dict:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["summary", "spider_code", "deploy_code"],
        "properties": {
            "summary": {"type": "string"},
            "spider_code": {"type": "string"},
            "deploy_code": {"type": "string"},
        },
    }

    combined_prompt = (
        "请严格按给定 JSON Schema 输出最终结果。\n\n"
        "### System Instructions\n"
        f"{system_prompt}\n\n"
        "### User Request\n"
        f"{user_prompt}\n"
    )

    with tempfile.TemporaryDirectory(prefix="codex-helper-") as tmpdir:
        schema_path = Path(tmpdir) / "schema.json"
        output_path = Path(tmpdir) / "response.json"
        schema_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")

        cmd = [
            CODEX_BIN,
            "exec",
            "-",
            "--skip-git-repo-check",
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(output_path),
            "--sandbox",
            "read-only",
            "-C",
            str(PROJECT_ROOT),
        ]
        if CODEX_MODEL:
            cmd.extend(["-m", CODEX_MODEL])

        result = subprocess.run(
            cmd,
            input=combined_prompt,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "codex exec 执行失败")

        raw = output_path.read_text(encoding="utf-8").strip()
        return json.loads(raw)


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
                "codex_bin": CODEX_BIN,
                "model": CODEX_MODEL or None,
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
            data = _run_codex(system_prompt, user_prompt)
            _json_response(self, 200, data)
        except Exception as exc:
            _json_response(self, 500, {"error": str(exc)})

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"local codex helper listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()
