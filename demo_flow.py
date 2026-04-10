from __future__ import annotations

from datetime import datetime, timezone

from prefect import flow, get_run_logger, task


@task
def build_payload() -> dict[str, str]:
    return {
        "message": "demo flow started",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@flow(name="demo-flow")
def demo_flow() -> dict[str, str]:
    logger = get_run_logger()
    payload = build_payload()
    logger.info("payload=%s", payload)
    return payload


if __name__ == "__main__":
    print(demo_flow())
