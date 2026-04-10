from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from scrapy import Spider, signals
from scrapy.crawler import CrawlerProcess


class QuotesSpider(Spider):
    name = "quotes-demo"
    start_urls = ["https://quotes.toscrape.com/"]

    custom_settings = {
        "LOG_LEVEL": "ERROR",
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_TIMEOUT": 20,
    }

    def parse(self, response):
        for quote in response.css("div.quote"):
            yield {
                "text": quote.css("span.text::text").get(default="").strip("“”"),
                "author": quote.css("small.author::text").get(default=""),
                "tags": quote.css("div.tags a.tag::text").getall(),
            }


@task
def run_scrapy_demo() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
    crawler = process.create_crawler(QuotesSpider)

    def on_item_scraped(item: Any, response: Any, spider: Any) -> None:
        items.append(dict(item))

    crawler.signals.connect(on_item_scraped, signal=signals.item_scraped)
    process.crawl(crawler)
    process.start(stop_after_crawl=True, install_signal_handlers=False)

    return items


@flow(name="scrapy-demo-flow")
def scrapy_demo_flow() -> dict[str, Any]:
    logger = get_run_logger()
    items = run_scrapy_demo()
    logger.info("scraped %s quotes", len(items))
    if items:
        logger.info("first item=%s", items[0])

    return {
        "count": len(items),
        "first_author": items[0]["author"] if items else "",
    }


if __name__ == "__main__":
    print(scrapy_demo_flow())
