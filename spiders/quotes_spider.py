from __future__ import annotations

from prefect import flow, get_run_logger, task
from scrapling import Fetcher

from common.clickhouse_sink import save_data as ch_save_data
import time


SPIDER_NAME = "quotes_spider"
SOURCE_NAME = "quotes.toscrape.com"


@task(retries=3, retry_delay_seconds=5, name="提取名言数据")
def fetch_and_parse(url: str) -> list[dict]:
    logger = get_run_logger()
    logger.info("开始抓取 URL: %s", url)

    fetcher = Fetcher()
    page = fetcher.get(url)

    results: list[dict] = []
    quotes = page.css(".quote")

    for index, q in enumerate(quotes, start=1):
        text = q.css(".text::text").get()
        author = q.css(".author::text").get()
        tags = q.css(".tags .tag::text").getall()
        if not text:
            continue

        results.append(
            {
                "spider_name": SPIDER_NAME,
                "source_name": SOURCE_NAME,
                "item_type": "quote",
                "category": "名言",
                "title": text[:120],
                "text_content": text,
                "author": author,
                "tags": tags,
                "url": f"{url}#quote-{index}",
                "page_url": url,
                "extra": {
                    "quote_index": index,
                },
            }
        )

    logger.info("成功提取 %s 条数据", len(results))
    return results


@task(name="保存到数据库")
def save_data(data: list[dict], flow_run_id: str | None = None) -> int:
    ch_rows = []
    for item in data:
        url = item.get("url", "")
        ch_rows.append([url, SOURCE_NAME, item, time.time_ns(), int(time.time())])
    
    if ch_rows:
        ch_save_data(ch_rows)
    return len(data)


@flow(name="名言网站抓取_Scrapling版", log_prints=True)
def quotes_scraper_flow():
    from prefect.runtime import flow_run

    target_url = "http://quotes.toscrape.com/"
    scraped_data = fetch_and_parse(target_url)

    if scraped_data:
        saved = save_data(scraped_data, flow_run.id)
        get_run_logger().info("已写入通用表 %s 条", saved)


if __name__ == "__main__":
    quotes_scraper_flow()

