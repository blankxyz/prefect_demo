from __future__ import annotations

import hashlib
import html as html_lib
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

try:
    from prefect import flow, get_run_logger, task
except Exception:  # pragma: no cover - local fallback when prefect is unavailable
    _fallback_logger = logging.getLogger("gdj_henan_tz_spider")
    if not _fallback_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        _fallback_logger.addHandler(handler)
    _fallback_logger.setLevel(logging.INFO)

    def get_run_logger():
        return _fallback_logger

    def task(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

    def flow(*_args, **_kwargs):
        def _decorator(fn):
            return fn

        return _decorator

from scrapling import Fetcher

from common.clickhouse_sink import filter_new_items_by_url
from common.nrta_base import extract_detail_content
from common.result_sink import save_items_to_sinks
from common.spider_common import (
    extract_meta,
    format_iso_z,
    normalize_multiline_text,
    normalize_text,
    now_iso_z,
    parse_datetime,
    publish_date_iso,
)

LIST_URL = "https://gd.henan.gov.cn/sy/tz/"
SITE_DOMAIN = "gd.henan.gov.cn"
ACCOUNT_CODE = "9_HN_GDJ_TZ_00_410000"
CONTENT_TYPE = "t_notice"
SPIDER_ID = "spider"
FIRST_PAGE_LIMIT = 20
REQUEST_TIMEOUT = 25
KAFKA_BOOTSTRAP_SERVERS = ["59.110.20.108:19092", "59.110.21.25:19092", "47.93.84.177:19092"]
KAFKA_TOPIC = "all_industry_data"

DATE_RE = re.compile(r"(20\d{2})[./-年](\d{1,2})[./-月](\d{1,2})")
DATETIME_RE = re.compile(
    r"(20\d{2})[./-年](\d{1,2})[./-月](\d{1,2})(?:[日\sT]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?"
)

LIST_ITEM_PATTERNS = [
    re.compile(
        r"<li[^>]*>.*?<a[^>]+href=[\"'](?P<href>[^\"']+)[\"'][^>]*"
        r"(?:title=[\"'](?P<title_attr>[^\"']+)[\"'])?[^>]*>(?P<title_text>.*?)</a>"
        r".*?(?P<date>20\d{2}[./-]\d{1,2}[./-]\d{1,2})?.*?</li>",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"<a[^>]+href=[\"'](?P<href>[^\"']+)[\"'][^>]*"
        r"(?:title=[\"'](?P<title_attr>[^\"']+)[\"'])?[^>]*>(?P<title_text>.*?)</a>"
        r"(?:.{0,180}?)(?P<date>20\d{2}[./-]\d{1,2}[./-]\d{1,2})",
        re.IGNORECASE | re.DOTALL,
    ),
]
ANCHOR_RE = re.compile(
    r"<a\b[^>]*\bhref\s*=\s*(?P<q>[\"']?)(?P<href>[^\"'>\s]+)(?P=q)[^>]*>(?P<title>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
ARTICLE_LIST_BLOCK_RE = re.compile(
    r"<div[^>]+class=[\"'][^\"']*\barticle_List\b[^\"']*[\"'][^>]*>(?P<body>.*?)</div>",
    re.IGNORECASE | re.DOTALL,
)
PARAGRAPH_RE = re.compile(r"<p\b[^>]*>(?P<body>.*?)</p>", re.IGNORECASE | re.DOTALL)


def _build_fetcher() -> Fetcher:
    return Fetcher()


def _strip_tags(value: str) -> str:
    return normalize_text(re.sub(r"<[^>]+>", " ", value or ""))


def _normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    match = DATE_RE.search(value)
    if not match:
        return None
    y, m, d = match.groups()
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def _normalize_datetime(value: str | None) -> str | None:
    if not value:
        return None
    match = DATETIME_RE.search(value)
    if not match:
        return None
    y, m, d, hh, mm, ss = match.groups()
    if hh is None or mm is None:
        return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
    sec = int(ss) if ss is not None else 0
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d} {int(hh):02d}:{int(mm):02d}:{sec:02d}"


def _is_valid_detail_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    if not parsed.netloc.endswith(SITE_DOMAIN):
        return False
    path = (parsed.path or "").lower()
    if ".html" not in path and ".shtml" not in path:
        return False
    return True


def _generate_tbid(url: str) -> int:
    m = re.search(r"(\d{6,})", url)
    if m:
        return int(m.group(1)[-18:])
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
    return int(digest, 16)


def _extract_list_entries(html: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _append_result(url: str, title: str, published_date: str | None) -> None:
        if not url or not title:
            return
        if url in seen:
            return
        seen.add(url)
        results.append(
            {
                "url": url,
                "title": title,
                "published_date": published_date,
                "tbid": _generate_tbid(url),
                "page_url": LIST_URL,
                "list_position": len(results) + 1,
            }
        )

    # 优先解析公告列表容器，避免跨区域误匹配导致标题串联
    for block in ARTICLE_LIST_BLOCK_RE.finditer(html):
        block_html = block.group("body") or ""
        paragraphs = list(PARAGRAPH_RE.finditer(block_html))
        if not paragraphs:
            paragraphs = [re.match(r"(?P<body>.*)", block_html, flags=re.DOTALL)]  # type: ignore[list-item]
        for para in paragraphs:
            para_html = para.group("body") if para else ""
            date_match = DATE_RE.search(para_html)
            published_date = _normalize_date(date_match.group(0)) if date_match else None
            for anchor in ANCHOR_RE.finditer(para_html):
                href = normalize_text(anchor.group("href"))
                if not href:
                    continue
                url = urljoin(LIST_URL, href)
                if not _is_valid_detail_url(url):
                    continue
                title = _strip_tags(anchor.group("title") or "")
                if not title:
                    continue
                _append_result(url, title, published_date)

    if results:
        return results

    for pattern in LIST_ITEM_PATTERNS:
        for match in pattern.finditer(html):
            href = normalize_text(match.groupdict().get("href"))
            if not href:
                continue
            url = urljoin(LIST_URL, href)
            if not _is_valid_detail_url(url):
                continue
            if url in seen:
                continue

            title_attr = normalize_text(match.groupdict().get("title_attr"))
            title_text = _strip_tags(match.groupdict().get("title_text") or "")
            title = title_attr or title_text
            if not title:
                continue

            date_text = normalize_text(match.groupdict().get("date"))
            published_date = _normalize_date(date_text)

            _append_result(url, title, published_date)

    if results:
        return results

    for match in ANCHOR_RE.finditer(html):
        href = normalize_text(match.group("href"))
        url = urljoin(LIST_URL, href)
        if not _is_valid_detail_url(url):
            continue
        title = _strip_tags(match.group("title"))
        if not title:
            continue
        _append_result(url, title, None)

    return results


def _extract_meta_property(html: str, prop: str) -> str | None:
    m = re.search(
        rf"<meta\\s+property=[\"']{re.escape(prop)}[\"']\\s+content=[\"']([^\"']+)[\"']",
        html,
        re.IGNORECASE,
    )
    return normalize_text(m.group(1)) if m else None


def _extract_author(html: str) -> str:
    author = extract_meta(html, "author")
    if author:
        return author
    m = re.search(r"(?:作者|编辑|来源)\s*[:：]\s*([^<\s]{1,40})", html)
    return normalize_text(m.group(1)) if m else ""


def _html_to_text(fragment: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", fragment, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<(br|p|div|li|tr|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    return normalize_multiline_text(text)


@task(retries=2, retry_delay_seconds=5, name="抓取河南广电局通知列表")
def fetch_list_entries() -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    page = fetcher.get(LIST_URL, timeout=REQUEST_TIMEOUT)
    html = page.html_content

    entries = _extract_list_entries(html)
    entries = entries[:FIRST_PAGE_LIMIT]
    logger.info("第一页抓到 %s 条通知", len(entries))
    return entries


@task(retries=2, retry_delay_seconds=5, name="抓取河南广电局通知详情")
def fetch_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    results: list[dict[str, Any]] = []

    for idx, entry in enumerate(entries, start=1):
        try:
            page = fetcher.get(entry["url"], timeout=REQUEST_TIMEOUT)
            html = page.html_content

            title = (
                extract_meta(html, "ArticleTitle")
                or _extract_meta_property(html, "og:title")
                or entry.get("title")
                or ""
            )
            title = normalize_text(title)

            published_raw = (
                extract_meta(html, "pubDate")
                or extract_meta(html, "pubdate")
                or extract_meta(html, "PublishDate")
                or entry.get("published_date")
            )
            published_raw = _normalize_datetime(published_raw) or _normalize_datetime(html) or entry.get("published_date")
            published_dt = parse_datetime(published_raw)

            content = extract_detail_content(html)
            if not content:
                for selector in (
                    "div#zoom",
                    "div.TRS_Editor",
                    "div.TRSEditor",
                    "div#UCAP-CONTENT",
                    "div.article",
                    "div.content",
                    "div.zw",
                    "div#content",
                    "article",
                ):
                    nodes = page.css(selector)
                    if nodes and getattr(nodes[0], "text", None):
                        content = normalize_multiline_text(nodes[0].text)
                        if content:
                            break
            if not content:
                body_match = re.search(r"<body[^>]*>(?P<body>.*)</body>", html, re.IGNORECASE | re.DOTALL)
                body_html = body_match.group("body") if body_match else html
                content = _html_to_text(body_html)

            now_iso = now_iso_z()
            results.append(
                {
                    "spidertime": now_iso,
                    "content": content,
                    "publishtime": format_iso_z(published_dt),
                    "tbid": int(entry.get("tbid") or _generate_tbid(entry["url"])),
                    "url": entry["url"],
                    "author": _extract_author(html),
                    "title": title,
                    "accountcode": ACCOUNT_CODE,
                    "spiderid": SPIDER_ID,
                    "createtime": now_iso,
                    "publishdate": publish_date_iso(published_dt),
                    "type": CONTENT_TYPE,
                    "browsenum": 0,
                }
            )
            logger.info("[%s/%s] 详情抓取成功: %s", idx, len(entries), title or entry["url"])
        except Exception as exc:
            logger.warning("[%s/%s] 详情抓取失败: %s | %s", idx, len(entries), entry.get("url"), exc)

    logger.info("详情抓取完成，共产出 %s 条结果", len(results))
    return results


@task(name="打印河南广电局通知样例")
def print_results(items: list[dict[str, Any]]) -> None:
    for item in items[:8]:
        print(
            f"{item.get('publishdate') or '未知日期'} | "
            f"{item.get('title')} | "
            f"{item.get('url')}"
        )
    print(f"--- 共 {len(items)} 条 ---")


@flow(name="河南省广电局_通知公告_抓取", log_prints=True)
def gdj_henan_tz_flow() -> list[dict[str, Any]]:
    get_run_logger().info("按第一页模式抓取 河南广电局通知")
    entries = fetch_list_entries()
    new_entries = filter_new_items_by_url(entries, site_name=ACCOUNT_CODE)
    items = fetch_details(new_entries)

    if items:
        saved = save_items_to_sinks(
            items,
            site_name=ACCOUNT_CODE,
            topic=KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        get_run_logger().info("已写入通用表 %s 条", saved)
        print_results(items)

    return items


gdj_henan_tz_flow.interval = 86400


if __name__ == "__main__":
    gdj_henan_tz_flow()
