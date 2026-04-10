from __future__ import annotations

import re

from common.nrta_base import NRTASpiderConfig, build_nrta_flow


CONFIG = NRTASpiderConfig(
    flow_name="国家广电总局_工作动态_抓取",
    list_task_name="抓取NRTA工作动态列表",
    detail_task_name="抓取NRTA工作动态详情",
    print_task_name="打印NRTA工作动态样例",
    list_log_label="工作动态",
    mode_log_label="NRTA 工作动态",
    list_url="https://www.nrta.gov.cn/col/col114/index.html",
    account_code="9_GDZJ_HYDF_00_000000",
    content_type="t_industry",
    detail_url_re=re.compile(r"https://www\.nrta\.gov\.cn/art/\d{4}/\d{1,2}/\d{1,2}/art_114_(\d+)\.html"),
    list_item_re=re.compile(
        r'<a href="(?P<url>https://www\.nrta\.gov\.cn/art/\d{4}/\d{1,2}/\d{1,2}/art_114_\d+\.html)"'
        r'[^>]*title="(?P<title>[^"]+)"[^>]*>.*?<span>(?P<date>\d{4}-\d{2}-\d{2})</span>',
        re.IGNORECASE | re.DOTALL,
    ),
)


nrta_activity_flow = build_nrta_flow(CONFIG)


if __name__ == "__main__":
    nrta_activity_flow()
