from __future__ import annotations

import re

from common.nrta_base import NRTASpiderConfig, build_nrta_flow


CONFIG = NRTASpiderConfig(
    flow_name="国家广电总局_总局要闻_抓取",
    list_task_name="抓取NRTA总局要闻列表",
    detail_task_name="抓取NRTA总局要闻详情",
    print_task_name="打印NRTA总局要闻样例",
    list_log_label="总局要闻",
    mode_log_label="NRTA 总局要闻",
    list_url="https://www.nrta.gov.cn/col/col112/index.html",
    account_code="9_GDZJ_ZJGZ_00_000000",
    content_type="t_industry",
    detail_url_re=re.compile(r"https://www\.nrta\.gov\.cn/art/\d{4}/\d{1,2}/\d{1,2}/art_112_(\d+)\.html"),
    list_item_re=re.compile(
        r'<a href="(?P<url>https://www\.nrta\.gov\.cn/art/\d{4}/\d{1,2}/\d{1,2}/art_112_\d+\.html)"'
        r'[^>]*>(?P<title>.*?)<span>(?P<date>\d{4}-\d{2}-\d{2})</span>',
        re.IGNORECASE | re.DOTALL,
    ),
)


nrta_headline_flow = build_nrta_flow(CONFIG)


if __name__ == "__main__":
    nrta_headline_flow()
