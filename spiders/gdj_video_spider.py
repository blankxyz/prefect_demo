from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass, asdict
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from prefect import flow, get_run_logger, task

from common.clickhouse_sink import save_data as ch_save_data
import time
import os
import clickhouse_connect
try:
    from patchright.async_api import async_playwright
except ImportError:  # pragma: no cover - fallback for local/dev environments
    from playwright.async_api import async_playwright


BASE_URL = "https://app.gdj.gansu.gov.cn"
SPIDER_NAME = "gdj_video_spider"
OLD_SECTION_URL = "https://gdj.gansu.gov.cn/gdj/c109217/video_item.shtml"
SECTION_SEEDS = (("陇上精品", OLD_SECTION_URL),)
SECTION_TEXT_HINTS = (
    "本土纪录片",
    "纪录甘肃",
    "优秀公益广告",
    "历年优秀",
    "主题分类",
    "视听之窗",
    "2025年全省原创网络视听节目展播",
)
VIDEO_HREF_RE = re.compile(r"/home/news/detail/aid/\d+\.html$")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


@dataclass
class VideoItem:
    category: str
    title: str
    url: str
    date: str | None = None
    page_url: str | None = None
    thumbnail: str | None = None


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def _canonical_list_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query.pop("p", None)
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def _looks_like_section_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return (
        "video_item" in path
        and "detail/aid" not in path
        and parsed.netloc.endswith(("gdj.gansu.gov.cn",))
    )


async def _discover_section_links(page) -> list[tuple[str, str]]:
    anchors = await page.locator("a").evaluate_all(
        """
        (els) => els.map((el) => ({
          href: el.href || "",
          text: (el.innerText || el.textContent || "").replace(/\\s+/g, " ").trim(),
        }))
        """
    )

    discovered: list[tuple[str, str]] = []
    seen: set[str] = set()

    for anchor in anchors:
        href = anchor.get("href") or ""
        text = _normalize_text(anchor.get("text"))
        if not href or not text:
            continue
        if text in {"上一页", "下一页"} or re.fullmatch(r"[0-9]+", text):
            continue
        if "detail/aid" in href:
            continue
        if not _looks_like_section_url(href) and not any(hint in text for hint in SECTION_TEXT_HINTS):
            continue

        canonical = _canonical_list_url(href)
        if canonical in seen:
            continue
        seen.add(canonical)
        discovered.append((text, href))

    return discovered


def _extract_items_from_payload(payload: dict[str, Any], category: str, page_url: str) -> list[VideoItem]:
    data = payload.get("data") or {}
    results = data.get("results") or []

    items: list[VideoItem] = []
    seen: set[str] = set()

    for result in results:
        if not isinstance(result, dict):
            continue

        raw_url = _normalize_text(result.get("url"))
        if not raw_url:
            continue

        title = _normalize_text(result.get("title")) or _normalize_text(result.get("subTitle"))
        if not title or raw_url in seen:
            continue

        res_list = result.get("resList") or []
        thumbnail = None
        if isinstance(res_list, list) and res_list:
            first_res = res_list[0] if isinstance(res_list[0], dict) else {}
            file_path = first_res.get("filePathNew") or first_res.get("filePath")
            if file_path:
                thumbnail = urljoin(BASE_URL, file_path)

        published = _normalize_text(result.get("publishedTimeStr"))
        if not published:
            published = _normalize_text(result.get("publishedTime"))

        items.append(
            VideoItem(
                category=category,
                title=title,
                url=urljoin(BASE_URL, raw_url),
                date=published[:10] if published else None,
                page_url=page_url,
                thumbnail=thumbnail,
            )
        )
        seen.add(raw_url)

    return items


async def _load_payload(page, url: str) -> tuple[dict[str, Any], str]:
    async with page.expect_response(lambda r: "/common/search/" in r.url) as response_info:
        await page.goto(url, wait_until="domcontentloaded")
    response = await response_info.value
    payload = await response.json()
    return payload, response.url


async def _crawl_category(category: str, start_url: str, max_pages: int | None = None) -> list[VideoItem]:
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=UA,
            ignore_https_errors=True,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = await context.new_page()
        page.set_default_timeout(30000)

        all_items: list[VideoItem] = []
        seen_urls: set[str] = set()
        current_url = start_url
        page_num = 1
        payload, response_url = await _load_payload(page, current_url)

        while True:
            payload_category = _normalize_text((payload.get("data") or {}).get("channelName")) or category
            current_items = _extract_items_from_payload(payload, payload_category, current_url)
            new_count = 0
            for item in current_items:
                if item.url not in seen_urls:
                    all_items.append(item)
                    seen_urls.add(item.url)
                    new_count += 1

            print(
                f"[{category}] 第 {page_num} 页抓到 {len(current_items)} 条，"
                f"新增 {new_count} 条，累计 {len(all_items)} 条"
            )

            data = payload.get("data") or {}
            total = int(data.get("total") or 0)
            rows = int(data.get("rows") or len(current_items) or 20)
            total_pages = max(1, math.ceil(total / rows))
            if max_pages is not None:
                total_pages = min(total_pages, max_pages)
            if page_num >= total_pages:
                break

            next_link = page.get_by_role("link", name="下一页")
            if await next_link.count() == 0:
                break

            try:
                async with page.expect_response(lambda r: "/common/search/" in r.url and r.url != response_url) as response_info:
                    await next_link.first.click()
                response = await response_info.value
                payload = await response.json()
                response_url = response.url
            except Exception:
                break

            page_num += 1

        await browser.close()
        return all_items


async def _collect_section_entries() -> list[tuple[str, str]]:
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    discovered: dict[str, tuple[str, str]] = {}
    queue: list[tuple[str, str]] = list(SECTION_SEEDS)
    visited: set[str] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 2200},
            user_agent=UA,
            ignore_https_errors=True,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = await context.new_page()
        page.set_default_timeout(30000)

        while queue:
            category, url = queue.pop(0)
            canonical = _canonical_list_url(url)
            if canonical in visited:
                continue
            visited.add(canonical)

            try:
                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_timeout(1200)
            except Exception as exc:
                print(f"[发现] 跳过无法打开的栏目页 {url}: {exc}")
                continue

            discovered[canonical] = (category, url)

            for next_category, next_url in await _discover_section_links(page):
                next_canonical = _canonical_list_url(next_url)
                if next_canonical in visited or next_canonical in discovered:
                    continue
                queue.append((next_category, next_url))

        await browser.close()

    return list(discovered.values())


def spider_result_count_sync(spider_name: str) -> int:
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    try:
        client = clickhouse_connect.get_client(host=host, username='default', password='')
        res = client.query(f"SELECT count() FROM crawler_data WHERE site_name = '{spider_name}'")
        return res.result_rows[0][0]
    except Exception:
        return 0

@task(name="保存结果到数据库")
def save_results(items: list[dict[str, Any]], flow_run_id: str | None = None) -> int:
    ch_rows = []
    for item in items:
        url = item.get("url", "")
        ch_rows.append([url, SPIDER_NAME, item, time.time_ns(), int(time.time())])
    
    if ch_rows:
        ch_save_data(ch_rows)
    return len(items)


@task(retries=2, retry_delay_seconds=5, name="发现栏目入口")
def discover_sections() -> list[tuple[str, str]]:
    return asyncio.run(_collect_section_entries())


@task(retries=2, retry_delay_seconds=5, name="抓取单栏目")
def crawl_single_section(category: str, start_url: str, max_pages: int | None = None) -> list[dict[str, Any]]:
    items = asyncio.run(_crawl_category(category, start_url, max_pages=max_pages))
    return [asdict(item) for item in items]


def fetch_videos() -> list[dict[str, Any]]:
    logger = get_run_logger()
    logger.info("开始抓取甘肃广电局陇上精品视频分类")

    all_items: list[dict[str, Any]] = []
    seen: set[str] = set()
    existing_count = spider_result_count_sync(SPIDER_NAME)
    incremental_mode = existing_count > 0
    max_pages_per_section = 1 if incremental_mode else None

    if incremental_mode:
        logger.info("检测到历史结果 %s 条，本次按增量模式执行：每个栏目只抓第 1 页", existing_count)
    else:
        logger.info("未检测到历史结果，本次按全量模式执行：抓取所有栏目分页")

    section_entries = discover_sections()
    logger.info("发现 %s 个栏目入口", len(section_entries))
    for category, url in section_entries:
        logger.info("栏目: %s | %s", category, url)
        items = crawl_single_section(category, url, max_pages=max_pages_per_section)
        for item in items:
            if item["url"] in seen:
                continue
            all_items.append(item)
            seen.add(item["url"])

    logger.info("抓取完成，共 %s 条视频", len(all_items))
    return all_items


@task(name="打印结果")
def print_results(items: list[dict[str, Any]]):
    for item in items[:8]:
        print(
            f"{item['category']} | {item['date'] or '未知日期'} | "
            f"{item['title']} | {item['url']}"
        )
    print(f"--- 共 {len(items)} 条视频 ---")


@flow(name="甘肃广电局_陇上精品_视频抓取", log_prints=True)
def gdj_video_flow():
    from prefect.runtime import flow_run

    videos = fetch_videos()
    if videos:
        saved = save_results(videos, flow_run.id)
        get_run_logger().info("已写入通用表 %s 条", saved)
        print_results(videos)


if __name__ == "__main__":
    gdj_video_flow()
