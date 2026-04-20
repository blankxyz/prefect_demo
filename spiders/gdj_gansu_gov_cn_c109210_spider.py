from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from typing import Any
from urllib.parse import urljoin

try:
    from prefect import flow, get_run_logger, task
except Exception:  # pragma: no cover - local fallback when prefect is unavailable
    _fallback_logger = logging.getLogger("gdj_gansu_gov_cn_c109210_spider")
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

try:
    from patchright.async_api import async_playwright
except ImportError:  # pragma: no cover - fallback for local/dev environments
    from playwright.async_api import async_playwright

from common.clickhouse_sink import filter_new_items_by_url
from common.result_sink import save_items_to_sinks

ACCOUNT_CODE = "15_STWZ_GANSUGOVCN_00_620000"
BASE_URL = "https://gdj.gansu.gov.cn"
SITE_NAME = "甘肃省广播电视局"
SPIDER_NAME = "gdj-gansu-gov-cn-c109210-prefect"
KAFKA_BOOTSTRAP_SERVERS = ["59.110.20.108:19092", "59.110.21.25:19092", "47.93.84.177:19092"]
KAFKA_TOPIC = "all_industry_data"
REQUEST_TIMEOUT_MS = 12000
LIST_WAIT_STEPS_MS = (3000, 5000, 8000, 12000)
DETAIL_WAIT_MS = 6000
FIRST_PAGE_LIMIT = 20
DEFAULT_COLUMN = "本局消息"
SECTIONS = [
    {
        "url": "https://gdj.gansu.gov.cn/gdj/c109210/xwzxcdh.shtml",
        "column_name": "本局消息",
        "channel_id": "da0a67b533e44b5db010364acd9ee7bb",
        "path_hint": "/gdj/c109210/",
    },
    {
        "url": "https://gdj.gansu.gov.cn/gdj/c109213/xwzxcdh.shtml",
        "column_name": "行业动态",
        "channel_id": None,
        "path_hint": "/gdj/c109213/",
    },
]

TEXT_ITEM_KEYS = [
    "url",
    "project",
    "accountcode",
    "tbid",
    "spiderid",
    "author",
    "title",
    "publishdate",
    "publishtime",
    "spidertime",
    "content",
    "createtime",
    "type",
    "tags",
    "commentnum",
    "browsenum",
    "forwardnum",
    "likenum",
    "root_column_name",
    "column_name",
]

VIDEO_ITEM_KEYS = [
    "url",
    "project",
    "program_name",
    "content",
    "actor",
    "spider_time",
    "poster",
    "create_time",
    "publish_time",
    "director",
    "author",
    "source",
    "accountcode",
    "video_url",
    "root_column_name",
    "root_column_id",
    "column_id",
    "column_name",
    "program_id",
    "tags",
    "episode",
    "commentnum",
    "browsenum",
    "forwardnum",
    "likenum",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def file_name_md5(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def extract_publish_time(text: str, fallback: str = "") -> str:
    match = re.search(r"发布时间[:：]\s*(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?)", text or "")
    if match:
        return clean_text(match.group(1))
    match = re.search(r"20\d{2}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?", text or "")
    return clean_text(match.group(0)) if match else fallback


def extract_source(text: str, fallback: str = "") -> str:
    match = re.search(r"来源[:：]\s*([^\s]+(?:\s*[^\s：]+)?)", text or "")
    return clean_text(match.group(1)) if match else fallback


def extract_views(text: str) -> int:
    match = re.search(r"浏览次数[:：]?(\d+)", text or "")
    return int(match.group(1)) if match else 0


def extract_video_url_from_html(html: str, page_url: str) -> str:
    patterns = [
        r'"source"\s*:\s*"([^"]+)"',
        r'"mp4"\s*:\s*"([^"]+)"',
        r"source\s*:\s*'([^']+)'",
        r'<source[^>]+src=["\']([^"\']+)["\']',
        r'<video[^>]+src=["\']([^"\']+)["\']',
        r'<iframe[^>]+src=["\']([^"\']*(?:player|video)[^"\']*)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.I | re.S)
        if match:
            return urljoin(page_url, clean_text(match.group(1)).replace("\\/", "/"))
    return ""


def extract_poster_from_html(html: str, page_url: str) -> str:
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'"cover"\s*:\s*"([^"]+)"',
        r'"poster"\s*:\s*"([^"]+)"',
        r'<video[^>]+poster=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, re.I | re.S)
        if match:
            return urljoin(page_url, clean_text(match.group(1)).replace("\\/", "/"))
    return ""


def resolve_detail_type(entry: dict[str, Any], html: str, page_url: str) -> str:
    carried = clean_text(entry.get("content_type")).lower()
    if carried in {"video", "vod", "live"}:
        return "video"
    if "/video/" in page_url.lower():
        return "video"
    if extract_video_url_from_html(html, page_url):
        return "video"
    return "text"


def make_text_item(entry: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = clean_text(entry.get("title")) or clean_text(detail.get("title"))
    publish_time = clean_text(entry.get("publish_time")) or extract_publish_time(detail.get("body_text", ""), "")
    column_name = clean_text(entry.get("column_name")) or DEFAULT_COLUMN
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    item = {
        "url": detail["url"],
        "project": SPIDER_NAME,
        "accountcode": ACCOUNT_CODE,
        "tbid": file_name_md5(detail["url"]),
        "spiderid": SPIDER_NAME,
        "author": "",
        "title": title,
        "publishdate": publish_time,
        "publishtime": publish_time,
        "spidertime": now,
        "content": clean_text(detail.get("content") or detail.get("body_text")),
        "createtime": now,
        "type": "t_social_web",
        "tags": "textmessage",
        "commentnum": 0,
        "browsenum": 0,
        "forwardnum": 0,
        "likenum": 0,
        "root_column_name": column_name,
        "column_name": column_name,
    }
    assert list(item.keys()) == TEXT_ITEM_KEYS
    return item


def make_video_item(entry: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    title = clean_text(entry.get("title")) or clean_text(detail.get("title"))
    publish_time = clean_text(entry.get("publish_time")) or extract_publish_time(detail.get("body_text", ""), "")
    column_name = clean_text(entry.get("column_name")) or DEFAULT_COLUMN
    now = int(time.time())
    item = {
        "url": detail["url"],
        "project": SPIDER_NAME,
        "program_name": title,
        "content": clean_text(detail.get("content") or detail.get("body_text") or title),
        "actor": "",
        "spider_time": now,
        "poster": clean_text(detail.get("poster")),
        "create_time": now,
        "publish_time": publish_time,
        "director": "",
        "author": "",
        "source": SITE_NAME,
        "accountcode": ACCOUNT_CODE,
        "video_url": clean_text(detail.get("video_url")),
        "root_column_name": column_name,
        "root_column_id": "",
        "column_id": "",
        "column_name": column_name,
        "program_id": file_name_md5(detail["url"]),
        "tags": column_name,
        "episode": 1,
        "commentnum": 0,
        "browsenum": 0,
        "forwardnum": 0,
        "likenum": 0,
    }
    assert list(item.keys()) == VIDEO_ITEM_KEYS
    return item


async def _new_context(browser):
    return await browser.new_context(
        viewport={"width": 1440, "height": 2200},
        user_agent=UA,
        ignore_https_errors=True,
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
    )


async def _section_entries(context, section: dict[str, Any]) -> list[dict[str, Any]]:
    page = await context.new_page()
    page.set_default_timeout(REQUEST_TIMEOUT_MS)
    xhr_payload: dict[str, Any] | None = None

    async def on_response(resp):
        nonlocal xhr_payload
        channel_id = section.get("channel_id")
        if channel_id and f"/common/search/{channel_id}" not in resp.url:
            return
        if channel_id is None and "/common/search/" not in resp.url:
            return
        if resp.status != 200:
            return
        try:
            payload = await resp.json()
        except Exception:
            return
        if (payload.get("data") or {}).get("results"):
            xhr_payload = payload

    page.on("response", lambda resp: asyncio.create_task(on_response(resp)))
    try:
        await page.goto(section["url"], wait_until="domcontentloaded")
        selector = f".newList ul li .left p a[href*='{section['path_hint']}']"
        for wait_ms in LIST_WAIT_STEPS_MS:
            await page.wait_for_timeout(wait_ms)
            if xhr_payload is not None:
                break
            if await page.locator(selector).count() > 0:
                break

        entries: list[dict[str, Any]] = []
        seen: set[str] = set()

        if xhr_payload is not None:
            for index, row in enumerate((xhr_payload.get("data") or {}).get("results") or [], start=1):
                detail_url = urljoin(BASE_URL, row.get("url") or "")
                if not detail_url or detail_url in seen:
                    continue
                seen.add(detail_url)
                entries.append(
                    {
                        "url": detail_url,
                        "title": clean_text(row.get("title")),
                        "publish_time": clean_text(row.get("publishedTimeStr")),
                        "column_name": clean_text(row.get("channelName")) or section["column_name"],
                        "source": clean_text(row.get("source")),
                        "content_type": clean_text(row.get("contentType") or row.get("type") or "news"),
                        "list_position": index,
                        "page_url": section["url"],
                    }
                )
                if len(entries) >= FIRST_PAGE_LIMIT:
                    break
            return entries

        rows = await page.locator(".newList ul li").evaluate_all(
            f"""
            (els) => els.map((li, index) => {{
              const anchor = li.querySelector(".left p a[href*='{section['path_hint']}']") || li.querySelector("a[href*='{section['path_hint']}']");
              const timeEl = li.querySelector("em");
              return {{
                href: anchor ? (anchor.href || anchor.getAttribute('href') || '') : '',
                title: anchor ? ((anchor.innerText || anchor.textContent || '').replace(/\\s+/g, ' ').trim()) : '',
                publish_time: timeEl ? ((timeEl.innerText || timeEl.textContent || '').replace(/\\s+/g, ' ').trim()) : '',
                list_position: index + 1,
              }};
            }})
            """
        )
        for row in rows[:FIRST_PAGE_LIMIT]:
            detail_url = clean_text(row.get("href"))
            if not detail_url or detail_url in seen:
                continue
            seen.add(detail_url)
            entries.append(
                {
                    "url": detail_url,
                    "title": clean_text(row.get("title")),
                    "publish_time": clean_text(row.get("publish_time")),
                    "column_name": section["column_name"],
                    "source": "",
                    "content_type": "news",
                    "list_position": int(row.get("list_position") or 0),
                    "page_url": section["url"],
                }
            )
        return entries
    finally:
        await page.close()


async def _fetch_list_entries_async() -> list[dict[str, Any]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = await _new_context(browser)
        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for section in SECTIONS:
            section_entries = await _section_entries(context, section)
            for entry in section_entries:
                if entry["url"] in seen:
                    continue
                seen.add(entry["url"])
                entries.append(entry)
        await context.close()
        await browser.close()
        return entries


async def _fetch_detail_async(context, entry: dict[str, Any]) -> dict[str, Any]:
    page = await context.new_page()
    page.set_default_timeout(REQUEST_TIMEOUT_MS)
    try:
        await page.goto(entry["url"], wait_until="domcontentloaded")
        await page.wait_for_timeout(DETAIL_WAIT_MS)
        title = await page.locator("h6.text_title_f").first.inner_text() if await page.locator("h6.text_title_f").count() else await page.title()
        body_text = await page.evaluate("() => (document.body?.innerText || '').replace(/\\s+/g, ' ').trim()")
        content = await page.locator(".notice_content").first.inner_text() if await page.locator(".notice_content").count() else ""
        html = await page.content()
        return {
            "url": page.url,
            "title": clean_text(title),
            "body_text": clean_text(body_text),
            "content": clean_text(content),
            "html": html,
            "source": extract_source(body_text, clean_text(entry.get("source"))),
            "views": extract_views(body_text),
            "video_url": extract_video_url_from_html(html, page.url),
            "poster": extract_poster_from_html(html, page.url),
        }
    finally:
        await page.close()


async def _fetch_article_details_async(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-dev-shm-usage"])
        context = await _new_context(browser)
        items: list[dict[str, Any]] = []
        for index, entry in enumerate(entries, start=1):
            logger.info("[%s/%s] 开始抓取详情: %s", index, len(entries), entry.get("url"))
            try:
                detail = await _fetch_detail_async(context, entry)
                if resolve_detail_type(entry, detail["html"], detail["url"]) == "video":
                    items.append(make_video_item(entry, detail))
                    logger.info("[%s/%s] 详情抓取成功(video): %s", index, len(entries), detail.get("title") or entry.get("title"))
                else:
                    items.append(make_text_item(entry, detail))
                    logger.info("[%s/%s] 详情抓取成功(text): %s", index, len(entries), detail.get("title") or entry.get("title"))
            except Exception as exc:
                logger.warning("[%s/%s] 详情抓取失败: %s | %s", index, len(entries), entry.get("url"), exc)
        await context.close()
        await browser.close()
        return items


@task(retries=2, retry_delay_seconds=5, name="抓取甘肃广电局栏目第一页列表")
def fetch_list_entries() -> list[dict[str, Any]]:
    logger = get_run_logger()
    entries = asyncio.run(_fetch_list_entries_async())
    logger.info("共抓到 %s 条列表记录", len(entries))
    return entries


@task(retries=2, retry_delay_seconds=5, name="抓取甘肃广电局栏目详情")
def fetch_article_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    items = asyncio.run(_fetch_article_details_async(entries))
    logger.info("详情抓取完成，共生成 %s 条结果", len(items))
    return items


@task(name="打印甘肃广电局样例")
def print_results(items: list[dict[str, Any]]) -> None:
    for item in items[:8]:
        print(f"{item.get('root_column_name')} | {item.get('publishdate') or item.get('publish_time') or '未知日期'} | {item.get('title') or item.get('program_name')} | {item.get('url')}")
    print(f"--- 共 {len(items)} 条 ---")


@flow(name="甘肃省广电局_本局消息和行业动态_第一页抓取", log_prints=True)
def gdj_gansu_gov_cn_c109210_flow() -> list[dict[str, Any]]:
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


gdj_gansu_gov_cn_c109210_flow.interval = 86400


if __name__ == "__main__":
    gdj_gansu_gov_cn_c109210_flow()
