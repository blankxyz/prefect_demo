from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

from prefect import flow, get_run_logger, task
from scrapling import Fetcher

from common.clickhouse_sink import filter_new_items_by_url
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

MODULE_CODE = "rta_hebei_sjyw"
BASE_URL = "http://rta.hebei.gov.cn"
LIST_URL = "http://rta.hebei.gov.cn/lists/24/0.html"
ACCOUNT_CODE = "9_HB_HBSGDJ_SJYW_130000"
CONTENT_TYPE = "t_industry"
SPIDER_ID = MODULE_CODE
FIRST_PAGE_LIMIT = 20
REQUEST_TIMEOUT = 20
KAFKA_BOOTSTRAP_SERVERS = ["59.110.20.108:19092", "59.110.21.25:19092", "47.93.84.177:19092"]
KAFKA_TOPIC = "all_industry_data"
DATE_RE = re.compile(r"(20\d{2})[年\-/\.](\d{1,2})[月\-/\.](\d{1,2})(?:[日\sT]*(\d{1,2})[:时](\d{1,2})(?::(\d{1,2}))?)?")
DETAIL_URL_RE = re.compile(r"/details?/\d+/(\d+)\.html(?:\?.*)?$", re.IGNORECASE)

LIST_SELECTORS = [
    "ul li",
    "ol li",
    "div.list li",
    "div.news-list li",
    "div.news_list li",
    "div.article-list li",
    "div.listbox li",
    "div.column-list li",
]
DETAIL_TITLE_SELECTORS = [
    "h1",
    "div.article-title",
    "div.detail-title",
    "div.news_title",
    "div.content_title",
    "div.xl_title",
    "div.arc_tit",
]
DETAIL_TIME_SELECTORS = [
    "div.info",
    "div.article-info",
    "div.detail-info",
    "div.news_info",
    "span.time",
    "p.time",
]
DETAIL_CONTENT_SELECTORS = [
    "div#zoom",
    "div.TRS_Editor",
    "div.article-content",
    "div.detail-content",
    "div.news_content",
    "div.content",
    "div.content-main",
    "div.main-content",
    "div.pages_content",
    "div.xl_main",
    "div.article",
]
AUTHOR_RE = re.compile(r"(?:作者|来源|编辑|责任编辑)\s*[:：]\s*([^\n\r<>]{1,80})")


def _build_fetcher() -> Fetcher:
    return Fetcher()


def _extract_date_text(value: str | None) -> str | None:
    text = normalize_text(value)
    if not text:
        return None
    match = DATE_RE.search(text)
    if not match:
        return None
    year, month, day, hour, minute, second = match.groups()
    if hour is None or minute is None:
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return (
        f"{year}-{int(month):02d}-{int(day):02d} "
        f"{int(hour):02d}:{int(minute):02d}:{int(second or '0'):02d}"
    )


def _generate_tbid(seed: int = 0) -> int:
    return time.time_ns() + seed


def _extract_tbid(url: str, *, fallback_seed: int = 0) -> int:
    match = DETAIL_URL_RE.search(url)
    if match:
        return int(match.group(1))
    digit_match = re.search(r"(\d{6,})", url)
    if digit_match:
        return int(digit_match.group(1))
    return _generate_tbid(fallback_seed)


def _is_http_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _looks_like_detail_url(url: str) -> bool:
    if not _is_http_url(url):
        return False
    if url.rstrip("/") == LIST_URL.rstrip("/"):
        return False
    parsed = urlparse(url)
    if parsed.netloc != urlparse(BASE_URL).netloc:
        return False
    path = parsed.path.lower()
    return path.endswith(".html") and ("/detail" in path or "/details" in path or path.count("/") >= 2)


def _pick_first_nonempty_text(page: Any, selectors: list[str]) -> str:
    for selector in selectors:
        nodes = page.css(selector)
        for node in nodes:
            text = normalize_text(getattr(node, "text", ""))
            if text:
                return text
    return ""


def _extract_author(html: str, page_text: str) -> str:
    meta_author = extract_meta(html, "author")
    if meta_author:
        return meta_author
    for source in (page_text, html):
        match = AUTHOR_RE.search(source or "")
        if match:
            return normalize_text(match.group(1))
    return ""


def _extract_content(page: Any) -> str:
    for selector in DETAIL_CONTENT_SELECTORS:
        nodes = page.css(selector)
        for node in nodes:
            text = normalize_multiline_text(getattr(node, "text", ""))
            if len(text) >= 20:
                return text

    html = getattr(page, "html_content", "") or ""
    content_match = re.search(
        r'<meta\s+name=["\']ContentStart["\']\s*/?>(?P<content>.*?)<meta\s+name=["\']ContentEnd["\']\s*/?>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    source_html = content_match.group("content") if content_match else html
    source_html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\\1>", "", source_html)
    source_html = re.sub(r"(?i)<br\s*/?>", "\n", source_html)
    source_html = re.sub(r"(?i)</?(p|div|li|tr|h1|h2|h3|h4|h5|h6)[^>]*>", "\n", source_html)
    source_html = re.sub(r"(?is)<[^>]+>", "", source_html)
    return normalize_multiline_text(source_html)


@task(retries=2, retry_delay_seconds=5, name="抓取河北广电局省局要闻列表")
def fetch_list_entries() -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    page = fetcher.get(LIST_URL, timeout=REQUEST_TIMEOUT)

    if getattr(page, "status", None) != 200:
        logger.error("列表页请求失败，状态码: %s", getattr(page, "status", "unknown"))
        return []

    li_nodes: list[Any] = []
    for selector in LIST_SELECTORS:
        li_nodes = page.css(selector)
        if li_nodes:
            logger.info("列表页命中选择器: %s", selector)
            break

    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for index, li in enumerate(li_nodes, start=1):
        links = li.css("a")
        if not links:
            continue

        article_url = ""
        title = ""
        for link in links:
            href = normalize_text(getattr(link, "attrib", {}).get("href", ""))
            if not href or href.lower().startswith("javascript:"):
                continue
            candidate_url = urljoin(BASE_URL, href)
            if not _looks_like_detail_url(candidate_url):
                continue
            article_url = candidate_url
            title = normalize_text(getattr(link, "attrib", {}).get("title") or getattr(link, "text", ""))
            if article_url and title:
                break

        if not article_url or article_url in seen_urls:
            continue

        combined_text = normalize_text(getattr(li, "text", ""))
        published_date = _extract_date_text(combined_text)

        item = {
            "url": article_url,
            "title": title,
            "published_date": published_date,
            "tbid": _extract_tbid(article_url, fallback_seed=index),
            "page_url": LIST_URL,
            "list_position": index,
        }
        seen_urls.add(article_url)
        items.append(item)

        if len(items) >= FIRST_PAGE_LIMIT:
            break

    logger.info("第一页抓到 %s 条省局要闻", len(items))
    return items


@task(retries=2, retry_delay_seconds=5, name="抓取河北广电局省局要闻详情")
def fetch_article_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    results: list[dict[str, Any]] = []

    for index, entry in enumerate(entries, start=1):
        now_iso = now_iso_z()
        try:
            page = fetcher.get(entry["url"], timeout=REQUEST_TIMEOUT)
            html = getattr(page, "html_content", "") or ""

            title = _pick_first_nonempty_text(page, DETAIL_TITLE_SELECTORS) or extract_meta(html, "ArticleTitle") or entry["title"]
            detail_time_text = _pick_first_nonempty_text(page, DETAIL_TIME_SELECTORS)
            published_raw = (
                extract_meta(html, "pubDate")
                or extract_meta(html, "pubdate")
                or _extract_date_text(detail_time_text)
                or _extract_date_text(html)
                or entry.get("published_date")
            )
            published_dt = parse_datetime(published_raw)
            page_text = normalize_multiline_text(re.sub(r"(?is)<[^>]+>", "\n", html))
            author = _extract_author(html, page_text)
            content = _extract_content(page)

            results.append(
                {
                    "spidertime": now_iso,
                    "content": content,
                    "publishtime": format_iso_z(published_dt),
                    "tbid": entry["tbid"],
                    "url": entry["url"],
                    "author": author,
                    "title": title,
                    "accountcode": ACCOUNT_CODE,
                    "spiderid": SPIDER_ID,
                    "createtime": now_iso,
                    "publishdate": publish_date_iso(published_dt),
                    "type": CONTENT_TYPE,
                    "browsenum": 0,
                }
            )
            logger.info("[%s/%s] 详情抓取成功: %s", index, len(entries), title)
        except Exception as exc:
            logger.warning("[%s/%s] 详情抓取失败: %s | %s", index, len(entries), entry["url"], exc)

    logger.info("详情抓取完成，共产出 %s 条结果", len(results))
    return results


@task(name="打印河北广电局省局要闻样例")
def print_results(items: list[dict[str, Any]]) -> None:
    for item in items[:8]:
        print(
            f"{item.get('publishdate') or '未知日期'} | "
            f"{item.get('title')} | "
            f"{item.get('url')}"
        )
    print(f"--- 共 {len(items)} 条 ---")


@flow(name="河北省广播电视局_省局要闻_抓取", log_prints=True)
def rta_hebei_sjyw_flow() -> list[dict[str, Any]]:
    get_run_logger().info("按第一页模式抓取 河北省广播电视局 省局要闻")
    entries = fetch_list_entries()
    new_entries = filter_new_items_by_url(entries, site_name=ACCOUNT_CODE)
    items = fetch_article_details(new_entries)

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


rta_hebei_sjyw_flow.interval = 86400


if __name__ == "__main__":
    rta_hebei_sjyw_flow()
