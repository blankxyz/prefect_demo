from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import re
from typing import Any, Pattern

from prefect import flow, get_run_logger, task
from scrapling import Fetcher

from common.clickhouse_sink import filter_new_items_by_url
from common.result_sink import save_items_to_sinks
from common.spider_common import (
    DEFAULT_USER_AGENT,
    extract_meta,
    format_iso_z,
    normalize_multiline_text,
    normalize_text,
    now_iso_z,
    parse_datetime,
    publish_date_iso,
)


@dataclass(frozen=True)
class NRTASpiderConfig:
    flow_name: str
    list_task_name: str
    detail_task_name: str
    print_task_name: str
    list_log_label: str
    mode_log_label: str
    list_url: str
    account_code: str
    content_type: str
    detail_url_re: Pattern[str]
    list_item_re: Pattern[str]
    first_page_limit: int = 15
    request_timeout: int = 20
    user_agent: str = DEFAULT_USER_AGENT
    spider_id: str = "spider"
    kafka_bootstrap_servers: tuple[str, ...] = (
        "59.110.20.108:19092",
        "59.110.21.25:19092",
        "47.93.84.177:19092",
    )
    kafka_topic: str = "all_industry_data"


class DetailContentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._ignored_tag_stack: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"style", "script"}:
            self._ignored_tag_stack.append(tag)
            return
        if self._ignored_tag_stack:
            return
        if tag == "br":
            self.parts.append("\n")
            return
        if tag in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if self._ignored_tag_stack and tag == self._ignored_tag_stack[-1]:
            self._ignored_tag_stack.pop()
            return
        if self._ignored_tag_stack:
            return
        if tag in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._ignored_tag_stack:
            return
        self.parts.append(data)


def extract_detail_content(html: str) -> str:
    match = re.search(
        r'<meta name="ContentStart"/>(?P<content>.*?)<meta name="ContentEnd"/>',
        html,
        re.IGNORECASE | re.DOTALL,
    )
    fragment = match.group("content") if match else ""
    parser = DetailContentParser()
    parser.feed(fragment)
    return normalize_multiline_text("".join(parser.parts))


def extract_list_entries(html: str, *, page_url: str, list_item_re: Pattern[str], detail_url_re: Pattern[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, match in enumerate(list_item_re.finditer(html), start=1):
        article_url = normalize_text(match.group("url"))
        title = normalize_text(match.group("title"))
        published_date = normalize_text(match.group("date"))
        tbid_match = detail_url_re.search(article_url)
        if not article_url or not title or not tbid_match:
            continue
        items.append(
            {
                "url": article_url,
                "title": title,
                "published_date": published_date,
                "tbid": int(tbid_match.group(1)),
                "page_url": page_url,
                "list_position": index,
            }
        )
    return items


def fetch_html(url: str, *, timeout: int) -> str:
    page = Fetcher().get(url, timeout=timeout)
    return page.html_content


def build_nrta_flow(config: NRTASpiderConfig):
    @task(retries=2, retry_delay_seconds=5, name=config.list_task_name)
    def fetch_entries() -> list[dict[str, Any]]:
        logger = get_run_logger()
        html = fetch_html(config.list_url, timeout=config.request_timeout)
        items = extract_list_entries(
            html,
            page_url=config.list_url,
            list_item_re=config.list_item_re,
            detail_url_re=config.detail_url_re,
        )[: config.first_page_limit]
        logger.info("第一页抓到 %s 条%s", len(items), config.list_log_label)
        return items

    @task(retries=2, retry_delay_seconds=5, name=config.detail_task_name)
    def fetch_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        logger = get_run_logger()
        results: list[dict[str, Any]] = []

        for index, entry in enumerate(entries, start=1):
            now_iso = now_iso_z()
            try:
                html = fetch_html(entry["url"], timeout=config.request_timeout)
                title = extract_meta(html, "ArticleTitle") or entry["title"]
                author = extract_meta(html, "author") or ""
                published_dt = parse_datetime(
                    extract_meta(html, "pubDate")
                    or extract_meta(html, "pubdate")
                    or entry["published_date"]
                )
                content = extract_detail_content(html)

                results.append(
                    {
                        "spidertime": now_iso,
                        "content": content,
                        "publishtime": format_iso_z(published_dt),
                        "tbid": entry["tbid"],
                        "url": entry["url"],
                        "author": author,
                        "title": title,
                        "accountcode": config.account_code,
                        "spiderid": config.spider_id,
                        "createtime": now_iso,
                        "publishdate": publish_date_iso(published_dt),
                        "type": config.content_type,
                        "browsenum": 0,
                    }
                )
                logger.info("[%s/%s] 详情抓取成功: %s", index, len(entries), title)
            except Exception as exc:
                logger.warning("[%s/%s] 详情抓取失败: %s | %s", index, len(entries), entry["url"], exc)

        logger.info("详情抓取完成，共产出 %s 条结果", len(results))
        return results

    @task(name=config.print_task_name)
    def print_results(items: list[dict[str, Any]]) -> None:
        for item in items[:8]:
            print(
                f"{item.get('publishdate') or '未知日期'} | "
                f"{item.get('title')} | "
                f"{item.get('url')}"
            )
        print(f"--- 共 {len(items)} 条 ---")

    @flow(name=config.flow_name, log_prints=True)
    def nrta_flow() -> list[dict[str, Any]]:
        get_run_logger().info("按第一页模式抓取 %s", config.mode_log_label)
        entries = fetch_entries()
        new_entries = filter_new_items_by_url(entries, site_name=config.account_code)
        items = fetch_details(new_entries)
        if items:
            saved = save_items_to_sinks(
                items,
                site_name=config.account_code,
                topic=config.kafka_topic,
                bootstrap_servers=list(config.kafka_bootstrap_servers),
            )
            get_run_logger().info("已写入通用表 %s 条", saved)
            print_results(items)
        return items

    nrta_flow.interval = 86400
    return nrta_flow
