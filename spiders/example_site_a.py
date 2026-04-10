from prefect import flow, task
from scrapling import StealthyFetcher
from common.clickhouse_sink import save_data
import time

@task
def scrape_page(url):
    # 初始化 Scrapling 隐身模式 [7]
    fetcher = StealthyFetcher()
    # 启用自适应追踪和 Cloudflare 破解 [8]
    result = fetcher.fetch(url, adaptive=True, solve_cloudflare=True)
    
    return {
        "url": url,
        "content": result.css('.content').text,
        "metadata": result.css('.meta').get_all()
    }

@flow(name="爬虫: 站点A", log_prints=True)
def site_a_flow(urls: list):
    results = []
    for url in urls:
        item = scrape_page(url)
        # 构造 ClickHouse 格式
        results.append([item['url'], "站点A", item, time.time_ns(), int(time.time())])
    
    save_data(results)

# 自定义部署属性 (供自动化脚本读取)
site_a_flow.interval = 3600  # 每小时运行一次
