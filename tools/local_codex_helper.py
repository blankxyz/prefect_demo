from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HOST = os.getenv("LOCAL_CODEX_HELPER_HOST", "0.0.0.0")
PORT = int(os.getenv("LOCAL_CODEX_HELPER_PORT", "8789"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/blank/playground/prefect_demo")).resolve()
CODEX_BIN = os.getenv("CODEX_BIN", "codex")
CODEX_MODEL = os.getenv("CODEX_MODEL", "").strip()

DEFAULT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["summary", "spider_code", "deploy_code"],
    "properties": {
        "summary": {"type": "string"},
        "spider_code": {"type": "string"},
        "deploy_code": {"type": "string"},
    },
}


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


def _sse_bytes(event: str, data: dict) -> bytes:
    line = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    return line.encode("utf-8")


def _build_combined_prompt(system_prompt: str, user_prompt: str) -> str:
    return (
        "请严格按给定 JSON Schema 输出最终结果。\n\n"
        "### System Instructions\n"
        f"{system_prompt}\n\n"
        "### User Request\n"
        f"{user_prompt}\n"
    )


def _build_codex_cmd(schema_path: Path) -> list[str]:
    cmd = [
        CODEX_BIN,
        "exec",
        "-",
        "--skip-git-repo-check",
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        # output_path injected by caller
        "--sandbox",
        "read-only",
        "-C",
        str(PROJECT_ROOT),
    ]
    if CODEX_MODEL:
        cmd.extend(["-m", CODEX_MODEL])
    return cmd


def _run_codex(system_prompt: str, user_prompt: str) -> dict:
    combined_prompt = _build_combined_prompt(system_prompt, user_prompt)

    with tempfile.TemporaryDirectory(prefix="codex-helper-") as tmpdir:
        schema_path = Path(tmpdir) / "schema.json"
        output_path = Path(tmpdir) / "response.json"
        schema_path.write_text(json.dumps(DEFAULT_SCHEMA, ensure_ascii=False), encoding="utf-8")

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


def _stream_generate(
    handler: BaseHTTPRequestHandler,
    system_prompt: str,
    user_prompt: str,
    output_schema: dict | None = None,
) -> None:
    """流式执行 codex，逐行把 stdout/stderr 作为 SSE progress 事件推送，结束后发 result 事件。"""
    schema = output_schema if isinstance(output_schema, dict) else DEFAULT_SCHEMA
    combined_prompt = _build_combined_prompt(system_prompt, user_prompt)

    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-cache")
    handler.send_header("Connection", "keep-alive")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()

    start_time = time.time()

    try:
        with tempfile.TemporaryDirectory(prefix="codex-stream-") as tmpdir:
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

            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=PROJECT_ROOT,
            )
            process.stdin.write(combined_prompt)
            process.stdin.close()

            for line in process.stdout:
                text = line.rstrip("\n\r")
                if not text:
                    continue
                elapsed = round(time.time() - start_time, 1)
                handler.wfile.write(_sse_bytes("progress", {"message": text, "elapsed": elapsed}))
                handler.wfile.flush()

            process.wait()

            if process.returncode != 0:
                handler.wfile.write(
                    _sse_bytes("error", {"message": f"codex 退出码 {process.returncode}"})
                )
                handler.wfile.flush()
                return

            if not output_path.exists():
                handler.wfile.write(_sse_bytes("error", {"message": "codex 未写出结果文件"}))
                handler.wfile.flush()
                return

            raw = output_path.read_text(encoding="utf-8").strip()
            result = json.loads(raw)
            handler.wfile.write(_sse_bytes("result", result))
            handler.wfile.flush()

    except Exception as exc:
        try:
            handler.wfile.write(_sse_bytes("error", {"message": str(exc)}))
            handler.wfile.flush()
        except Exception:
            pass


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
        if self.path not in {"/generate", "/generate-stream"}:
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

        if self.path == "/generate":
            try:
                data = _run_codex(system_prompt, user_prompt)
                _json_response(self, 200, data)
            except Exception as exc:
                _json_response(self, 500, {"error": str(exc)})
        else:
            output_schema = payload.get("output_schema") or None
            _stream_generate(self, system_prompt, user_prompt, output_schema)

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"local codex helper listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()
