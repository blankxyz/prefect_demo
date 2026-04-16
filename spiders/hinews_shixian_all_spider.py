from __future__ import annotations

import hashlib
import logging
import re
import time
from typing import Any

try:
    from prefect import flow, get_run_logger, task
except Exception:  # pragma: no cover - local fallback when prefect is unavailable
    _fallback_logger = logging.getLogger("hinews_shixian_all_spider")
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

from scrapling.fetchers import Fetcher

from common.clickhouse_sink import filter_new_items_by_url
from common.result_sink import save_items_to_sinks


ACCOUNT_CODE = "15_STWZ_YNTVCN_02_530000"
BASE_URL = "https://www.hinews.cn"
API_BASE_URL = "https://rm-comapi-pc.hinews.cn"
LIST_API = f"{API_BASE_URL}/open-service/content/getNewsIndexManyLevelByUuidSplit"
SITE_ID = 12
DEFAULT_DETAIL_SECTION = "1044"
REQUEST_TIMEOUT = 30
KAFKA_BOOTSTRAP_SERVERS = ["59.110.20.108:19092", "59.110.21.25:19092", "47.93.84.177:19092"]
KAFKA_TOPIC = "all_industry_data"
SPIDER_NAME = "hinews-cn-shixian-all-prefect"
SECTIONS = [
    {"column_name": "海口", "city": "1", "uuid": "97894cbd59064f64b938c7a3e6dade8a"},
    {"column_name": "三亚", "city": "2", "uuid": "7046b7062890493fbb5235e117ab243b"},
    {"column_name": "儋州", "city": "4", "uuid": "980e76a85d9d4dbeacff2be7ff002038"},
    {"column_name": "琼海", "city": "5", "uuid": "e01771117f384b10841780d9b274d1ab"},
    {"column_name": "文昌", "city": "6", "uuid": "3b9677a9f7894c8cb35274a6a3633e86"},
    {"column_name": "万宁", "city": "7", "uuid": "d3f48c84f8f547b7ac1a09e0c12798bb"},
    {"column_name": "东方", "city": "8", "uuid": "4e9625f240ae410fa2e2444235e7d4ba"},
    {"column_name": "五指山", "city": "9", "uuid": "b07b950221aa433b828173bf8a257e6f"},
    {"column_name": "乐东", "city": "10", "uuid": "46afb6e3bb53482e9613b351f933228c"},
    {"column_name": "澄迈", "city": "11", "uuid": "42cba3ea1625433b8f6298030496638d"},
    {"column_name": "临高", "city": "12", "uuid": "89a70430cfa9419d96cd801140289494"},
    {"column_name": "定安", "city": "13", "uuid": "68d1e6dacd214e8eb4356bc9a1f25d4b"},
    {"column_name": "屯昌", "city": "14", "uuid": "4ea19d9f390e4fd2b8a45423bed1c3be"},
    {"column_name": "陵水", "city": "15", "uuid": "bd85b3bbfd8a42faa8b90e30f00536c3"},
    {"column_name": "昌江", "city": "16", "uuid": "2585b17021e845d2a0e0e083c119b9a5"},
    {"column_name": "保亭", "city": "17", "uuid": "8be9e81175944209a049e6297137efa7"},
    {"column_name": "琼中", "city": "18", "uuid": "0df3ff26c1404930894bbfec8e5921e2"},
    {"column_name": "白沙", "city": "19", "uuid": "d1118183fb58428d98cca76ef873b59e"},
    {"column_name": "洋浦", "city": "20", "uuid": "621d2af0994748cd96fa7e35638af283"},
]


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def node_text(node) -> str:
    try:
        return clean(node.get_all_text(separator=" ", strip=True)) if node is not None else ""
    except Exception:
        return clean(getattr(node, "text", ""))


def first_text(response, selector: str) -> str:
    try:
        return node_text(response.css(selector).first)
    except Exception:
        return ""


def md5(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def api_json(response) -> dict[str, Any]:
    try:
        return response.json()
    except Exception:
        try:
            return response.json
        except Exception:
            body = response.body.decode("utf-8", errors="ignore") if getattr(response, "body", None) else ""
            try:
                import json

                return json.loads(body)
            except Exception:
                return {}


def nested_get(data: dict[str, Any], *keys: str) -> str:
    value: Any = data
    for key in keys:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    return clean(value)


def source_name(content: dict[str, Any]) -> str:
    source = content.get("source")
    if isinstance(source, dict):
        return clean(source.get("name"))
    return clean(source)


def extract_publish_time(response) -> str:
    meta_text = clean(first_text(response, "ul.page_brief"))
    match = re.search(r"(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)", meta_text)
    return clean(match.group(1)) if match else ""


def article_text(response, title: str) -> str:
    parts = [node_text(node) for node in response.css("#bs_content > div:first-child p, #bs_content .formatted, #bs_content p")]
    parts = [part for part in parts if part]
    if parts and clean(parts[0]) == clean(title):
        parts = parts[1:]
    return clean(" ".join(parts)) or first_text(response, "#bs_content > div:first-child") or first_text(response, "#bs_content")


def detail_url(wrapper: dict[str, Any], content: dict[str, Any]) -> str:
    for candidate in (
        nested_get(content, "url"),
        nested_get(content, "href"),
        nested_get(content, "pageUrl"),
        nested_get(content, "shareUrl"),
        nested_get(wrapper, "url"),
        nested_get(wrapper, "href"),
        nested_get(wrapper, "pageUrl"),
    ):
        if not candidate:
            continue
        return candidate if candidate.startswith("http") else f"{BASE_URL}{candidate}"

    news_id = ""
    for key in ("id", "newsId", "contentId", "contentNewsId", "n"):
        news_id = clean(content.get(key))
        if news_id:
            break
    section = clean(content.get("siteLayoutModuleArticleStyleId")) or clean(content.get("s")) or DEFAULT_DETAIL_SECTION
    if news_id:
        return f"{BASE_URL}/page?n={news_id}&m=1&s={section}"
    return ""


def text_item(detail, entry: dict[str, Any]) -> dict[str, Any]:
    column_name = clean(entry.get("column_name"))
    title = clean(entry.get("title")) or first_text(detail, "h2.page_h2")
    publish_time = clean(entry.get("publish_time")) or extract_publish_time(detail)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    return {
        "url": detail.url,
        "project": SPIDER_NAME,
        "accountcode": ACCOUNT_CODE,
        "tbid": md5(detail.url),
        "spiderid": SPIDER_NAME,
        "author": "",
        "title": title,
        "publishdate": publish_time,
        "publishtime": publish_time,
        "spidertime": now,
        "content": article_text(detail, title),
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


def _build_fetcher() -> Fetcher:
    return Fetcher()


@task(name="抓取Hinews市县第一页列表", retries=2, retry_delay_seconds=5)
def fetch_list_entries() -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    entries: list[dict[str, Any]] = []

    for section in SECTIONS:
        source_url = f"{BASE_URL}/column/{section['uuid']}?city={section['city']}"
        response = fetcher.post(
            LIST_API,
            data={"uuid": section["uuid"], "pageNo": 1, "siteId": SITE_ID},
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": BASE_URL,
                "Referer": source_url,
            },
            timeout=REQUEST_TIMEOUT,
            google_search=False,
        )
        rows = (api_json(response).get("data") or {}).get("list", []) or []
        seen: set[str] = set()
        for row in rows:
            wrapper = row.get("moduleNewsDataBO") or {}
            content = wrapper.get("contentNewsBO") or {}
            url = detail_url(wrapper, content)
            if not url or url in seen:
                continue
            seen.add(url)
            publish_time = clean(content.get("publishTime"))
            if not publish_time:
                stamp = clean(content.get("timeStamp"))
                if re.fullmatch(r"\d{10,13}", stamp):
                    try:
                        publish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(stamp[:10])))
                    except Exception:
                        publish_time = ""
            entries.append(
                {
                    "url": url,
                    "title": clean(content.get("title")),
                    "publish_time": publish_time,
                    "source": source_name(content),
                    "column_name": section["column_name"],
                    "section_uuid": section["uuid"],
                    "city": section["city"],
                }
            )
        logger.info("[%s] 第一页抓到 %s 条", section["column_name"], len(seen))

    logger.info("所有市县第一页共抓到 %s 条列表记录", len(entries))
    return entries


@task(name="抓取Hinews市县详情", retries=2, retry_delay_seconds=5)
def fetch_details(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger = get_run_logger()
    fetcher = _build_fetcher()
    results: list[dict[str, Any]] = []

    for index, entry in enumerate(entries, start=1):
        try:
            detail = fetcher.get(entry["url"], timeout=REQUEST_TIMEOUT, google_search=False)
            item = text_item(detail, entry)
            results.append(item)
            logger.info("[%s/%s] 详情抓取成功: %s | %s", index, len(entries), entry["column_name"], item["title"])
        except Exception as exc:
            logger.warning("[%s/%s] 详情抓取失败: %s | %s", index, len(entries), entry.get("url"), exc)

    logger.info("详情抓取完成，共产出 %s 条结果", len(results))
    return results


@task(name="打印Hinews市县样例")
def print_results(items: list[dict[str, Any]]) -> None:
    for item in items[:8]:
        print(
            f"{item.get('column_name')} | "
            f"{item.get('publishdate') or '未知日期'} | "
            f"{item.get('title')} | "
            f"{item.get('url')}"
        )
    print(f"--- 共 {len(items)} 条 ---")


@flow(name="Hinews_市县_第一页抓取", log_prints=True)
def hinews_shixian_all_flow() -> list[dict[str, Any]]:
    logger = get_run_logger()
    logger.info("按每个市县 section 的第一页模式抓取 Hinews 市县频道")
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
        logger.info("已写入通用表 %s 条", saved)
        print_results(items)
    else:
        logger.info("没有新数据需要写入")

    return items


hinews_shixian_all_flow.interval = 86400


if __name__ == "__main__":
    hinews_shixian_all_flow()
