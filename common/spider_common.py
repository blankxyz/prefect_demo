from __future__ import annotations

import re
from datetime import datetime, timezone
from html import unescape

import requests


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def normalize_text(value: str | None) -> str:
    text = unescape(value or "")
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_multiline_text(value: str | None) -> str:
    lines = [normalize_text(line) for line in (value or "").splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def extract_meta(html: str, name: str) -> str | None:
    match = re.search(
        rf'<meta\s+name=["\']{re.escape(name)}["\']\s+content=["\']([^"\']*)["\']',
        html,
        re.IGNORECASE,
    )
    if not match:
        return None
    return normalize_text(match.group(1))


def parse_datetime(value: str | None) -> datetime | None:
    normalized = normalize_text(value)
    if not normalized:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def format_iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def publish_date_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    midnight = datetime(dt.year, dt.month, dt.day)
    return format_iso_z(midnight)


def now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def build_requests_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
    )
    return session


def fetch_text(session: requests.Session, url: str, timeout: int = 20) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding or "utf-8"
    return response.text
