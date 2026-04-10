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
    format_iso_z,
    now_iso_z,
    parse_datetime,
    publish_date_iso,
)

BASE_URL = "https://gdj.ln.gov.cn"
LIST_URL = "https://gdj.ln.gov.cn/gdj/index/xydt/index.shtml"
ACCOUNT_CODE = "9_LN_LNSXWCBGDJ_00_210000"
CONTENT_TYPE = "t_industry"
SPIDER_ID = "spider"
KAFKA_BOOTSTRAP_SERVERS = ["59.110.20.108:19092", "59.110.21.25:19092", "47.93.84.177:19092"]
KAFKA_TOPIC = "all_industry_data"
DATE_RE = re.compile(r"(\d{4})[-年](\d{1,2})[-月](\d{1,2})")
FIRST_PAGE_LIMIT = 20


def _normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    match = DATE_RE.search(value)
    if not match:
        return None
    year, month, day = match.groups()
    return f"{year}-{int(month):02d}-{int(day):02d}"


def _is_internal_detail(url: str) -> bool:
    return urlparse(url).netloc.endswith("gdj.ln.gov.cn")


def _generate_tbid() -> int:
    return time.time_ns()


def _build_fetcher() -> Fetcher:
    return Fetcher()


def _extract_editor(page: Any) -> str:
    author_tags = page.css("p.gov_comeword")
    author_text = author_tags[0].text if author_tags and author_tags[0].text else ""
    editor = author_text.replace("编辑：", "").strip() if author_text else ""
    if editor:
        return editor

    page_html = page.html_content if hasattr(page, "html_content") else ""
    match = re.search(r"<p[^>]*class=[\"']gov_comeword[\"'][^>]*>\s*编辑：\s*([^<]+)</p>", page_html, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""


@task(retries=2, retry_delay_seconds=3, name="抓取辽宁行业动态列表")
def fetch_list_entries() -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    page = fetcher.get(LIST_URL, timeout=20)

    if getattr(page, "status", None) != 200:
        logger.error("列表页请求失败, 状态码: %s", getattr(page, "status", "unknown"))
        return []

    items: list[dict[str, Any]] = []
    li_elements = page.css("div.gxlListBox ul li")
    for index, li in enumerate(li_elements[:FIRST_PAGE_LIMIT], start=1):
        a_tags = li.css("a")
        span_tags = li.css("span")

        a = a_tags[0] if a_tags else None
        span = span_tags[0] if span_tags else None

        if not a:
            continue

        href = a.attrib.get("href", "")
        article_url = urljoin(BASE_URL, href)
        title_attr = a.attrib.get("title")
        title = title_attr if title_attr else (a.text.strip() if a.text else "")
        pub_date = _normalize_date(span.text) if span and span.text else None

        items.append(
            {
                "url": article_url,
                "title": title,
                "published_date": pub_date,
                "is_external": not _is_internal_detail(article_url),
                "list_position": index,
                "page_url": LIST_URL,
            }
        )

    internal_items = [item for item in items if not item["is_external"]]
    logger.info("第一页共抓到 %s 条，站内详情 %s 条", len(items), len(internal_items))
    return internal_items


@task(retries=2, retry_delay_seconds=3, name="抓取辽宁行业动态详情")
def fetch_article_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    results: list[dict[str, Any]] = []

    for index, entry in enumerate(entries, start=1):
        try:
            page = fetcher.get(entry["url"], timeout=20)

            h5_tags = page.css("h5")
            title = h5_tags[0].text.strip() if h5_tags and h5_tags[0].text else entry["title"]

            time_tags = page.css("p.gov_time")
            time_text = time_tags[0].text if time_tags and time_tags[0].text else None
            pub_date = _normalize_date(time_text) or entry.get("published_date")
            published_dt = parse_datetime(pub_date)

            author = _extract_editor(page)

            content_tags = page.css("div.TRS_Editor")
            raw_content = content_tags[0].text if content_tags and content_tags[0].text else ""
            content = re.sub(r"\.TRS_Editor\s*\{[^}]*\}", "", raw_content, flags=re.IGNORECASE).strip()

            now_iso = now_iso_z()
            results.append({
                "spidertime": now_iso,
                "content": content,
                "publishtime": format_iso_z(published_dt),
                "tbid": _generate_tbid(),
                "url": entry["url"],
                "author": author,
                "title": title,
                "accountcode": ACCOUNT_CODE,
                "spiderid": SPIDER_ID,
                "createtime": now_iso,
                "publishdate": publish_date_iso(published_dt),
                "type": CONTENT_TYPE,
                "browsenum": 0,
            })
            logger.info("[%s/%s] 详情抓取成功: %s", index, len(entries), title)
        except Exception as exc:
            logger.warning("[%s/%s] 详情抓取失败: %s | %s", index, len(entries), entry["url"], exc)

    return results


@task(name="打印辽宁行业动态样例")
def print_results(items: list[dict[str, Any]]) -> None:
    for item in items[:8]:
        print(
            f"{item.get('publishdate') or '未知日期'} | "
            f"{item.get('title')} | "
            f"{item.get('url')}"
        )
    print(f"--- 共 {len(items)} 条 ---")


@flow(name="辽宁省广电局_行业动态_抓取", log_prints=True)
def gdj_xydt_flow() -> list[dict[str, Any]]:
    records = fetch_list_entries()
    new_records = filter_new_items_by_url(records, site_name=ACCOUNT_CODE)
    detailed_records = fetch_article_details(new_records)

    if detailed_records:
        saved = save_items_to_sinks(
            detailed_records,
            site_name=ACCOUNT_CODE,
            topic=KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )
        get_run_logger().info("已写入通用表 %s 条", saved)
        print_results(detailed_records)

    return detailed_records


gdj_xydt_flow.interval = 86400


if __name__ == "__main__":
    gdj_xydt_flow()
