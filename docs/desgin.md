# 大规模分布式中文爬虫系统 (Prefect + Scrapling + ClickHouse)

## 1. 项目概述
本项目旨在构建一个企业级抓取平台。核心架构采用 **Prefect 3.x** 作为任务编排引擎，**Scrapling** 作为底层高对抗采集库，并利用 **ClickHouse** 实现海量 JSON 数据的高效存取与去重。针对中文环境，系统集成了 **Crawlab** UI 监控与 **钉钉/企业微信** 中文告警。

### 核心技术选型
*   **编排层**: Prefect 3.x (代码即调度，动态工作流) [1, 2]
*   **采集层**: Scrapling (TLS指纹伪装、AI自适应元素定位、Cloudflare 绕过) [3, 4]
*   **存储层**: ClickHouse 25.3+ (原生 JSON 类型支持、ReplacingMergeTree 自动去重) 
*   **UI/运维**: Crawlab (原生中文界面) [5, 6] + Prometheus/Grafana
*   **伸缩层**: Kubernetes + KEDA (基于任务积压的事件驱动缩放) 

---

## 2. 目录结构 (Monorepo 模式)
为了管理“一网一文件”的大规模脚本，采用以下标准结构：

crawler-platform/
├── common/                 # 共享组件
│   ├── clickhouse_sink.py  # ClickHouse 写入逻辑
│   ├── scrapling_base.py   # 通用采集配置
│   └── notifications.py    # 中文告警模版
├── spiders/                # 独立爬虫脚本仓
│   ├── example_site_a.py   # 站点 A 的独立文件
│   ├── example_site_b.py   # 站点 B 的独立文件
│   └── templates.py        # 新爬虫开发模板
├── deployments/            # 部署配置
│   ├── dockerfile          # 基础采集环境镜像
│   └── keda-scaled.yaml    # KEDA 扩缩容配置
├── deploy_all.py           # 自动化注册脚本
└── requirements.txt

---

## 3. 核心实现代码

### 3.1 数据库架构 (ClickHouse)
使用 `ReplacingMergeTree` 引擎确保在重试任务时数据自动去重，并开启异步写入以支撑高并发。

```sql
-- 在 ClickHouse 中执行
CREATE TABLE crawler_data (
    url String,
    site_name LowCardinality(String),
    raw_json JSON,                  -- 使用原生 JSON 类型 
    crawl_time DateTime64(3),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (site_name, url);

-- 开启服务器端异步攒批
ALTER USER default SETTINGS async_insert = 1, wait_for_async_insert = 1; 
```

### 3.2 通用写入组件 (`common/clickhouse_sink.py`)
```python
import clickhouse_connect
from prefect import task, get_run_logger

@task(name="数据落库", retries=3, retry_delay_seconds=30)
def save_data(data_list, table="crawler_data"):
    """
    data_list: list of dicts/tuples
    """
    logger = get_run_logger()
    # 实际应用中建议通过 Prefect Block 加载配置 
    client = clickhouse_connect.get_client(host='localhost', username='default', password='')
    
    try:
        client.insert(table, data_list)
        logger.info(f"成功写入 {len(data_list)} 条数据至 {table}")
    except Exception as e:
        logger.error(f"写入失败: {e}")
        raise
```

### 3.3 爬虫开发模版 (`spiders/example_site.py`)
每个站点作为一个独立文件，继承通用的采集逻辑。

```python
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
    results =
    for url in urls:
        item = scrape_page(url)
        # 构造 ClickHouse 格式
        results.append([item['url'], "站点A", item, time.time_ns(), int(time.time())])
    
    save_data(results)

# 自定义部署属性 (供自动化脚本读取)
site_a_flow.interval = 3600  # 每小时运行一次
```

---

## 4. 自动化注册逻辑 (`deploy_all.py`)
通过此脚本扫描 `spiders/` 目录，将所有 Python 文件自动注册为 Prefect Deployment。

```python
import os
import importlib.util
from prefect import deploy

def register_all_spiders():
    deployments =
    spider_dir = "./spiders"
    
    for file in os.listdir(spider_dir):
        if file.endswith(".py") and not file.startswith("__"):
            # 动态加载爬虫文件
            spec = importlib.util.spec_from_file_location(file[:-3], f"{spider_dir}/{file}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # 寻找模块中的 flow 对象
            for attr in dir(module):
                obj = getattr(module, attr)
                if hasattr(obj, "to_deployment"):
                    deployments.append(obj.to_deployment(
                        name=f"prod-{file[:-3]}",
                        interval=getattr(obj, "interval", None)
                    ))
    
    # 一键批量部署 
    deploy(
        *deployments,
        work_pool_name="k8s-crawler-pool",
        image="my-registry/scrapling-base:v1"
    )

if __name__ == "__main__":
    register_all_spiders()
```

---

## 5. 中文运维与监控实施

### 5.1 界面汉化 (Crawlab 集成)
由于 Prefect 官方 UI 暂未提供中文一键切换，推荐采用 **Crawlab** 作为展现层 [5, 6]：
1.  在 Kubernetes 中部署 Crawlab。
2.  在 Crawlab 中添加“执行节点”，指向 Prefect Worker。
3.  通过 Crawlab 的“自定义执行命令”，触发 Prefect 任务：
    `prefect deployment run '爬虫: 站点A/prod-example_site_a'`

### 5.2 中文自动化告警
创建 Prefect `CustomWebhookNotificationBlock` ，并配置钉钉机器人：
*   **Webhook URL**: `https://oapi.dingtalk.com/robot/send?access_token=xxx`
*   **JSON 模版**:
    ```json
    {
      "msgtype": "markdown",
      "markdown": {
        "title": "【爬虫告警】",
        "text": "### 任务运行失败 \n **流名称**: {{ name }} \n **错误详情**: {{ body }}"
      }
    }
    ```

---

## 6. 快速开始
1.  **安装环境**: `pip install prefect scrapling clickhouse-connect`
2.  **配置 Prefect**: `prefect server start`
3.  **初始化 Work Pool**: `prefect work-pool create --type kubernetes k8s-crawler-pool`
4.  **运行部署**: `python deploy_all.py`
5.  **启动 Worker**: `prefect worker start --pool k8s-crawler-pool`

---
**提示**: 在生产环境中，建议利用 **KEDA** 监控 Prometheus 暴露的 `prefect_flow_runs_scheduled` 指标，实现采集节点的零负载自动扩缩容 [9, 10]。

---

## 附件 A. ClickHouse 去重效率测试记录

### A.1 测试目的
验证当前 `ReplacingMergeTree(version) ORDER BY (site_name, url)` 表结构在重复 URL 场景下的去重效率，确认其是否会成为当前爬虫平台的性能瓶颈。

### A.2 测试环境
*   **数据库**: ClickHouse 26.3.2
*   **目标表结构**:

```sql
CREATE TABLE crawler_data (
    url String,
    site_name LowCardinality(String),
    raw_json JSON,
    crawl_time DateTime64(3),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (site_name, url);
```

### A.3 线上数据快照
测试时线上 `crawler_data` 表统计如下：

```sql
SELECT
    count() AS rows,
    uniqExact((site_name, url)) AS uniq_keys
FROM crawler_data;
```

结果：
*   总行数：`774`
*   唯一 `(site_name, url)` 键数：`759`

分站点统计：

```sql
SELECT
    site_name,
    count() AS rows,
    uniqExact(url) AS uniq_urls
FROM crawler_data
GROUP BY site_name
ORDER BY rows DESC;
```

结果摘要：
*   `gdj_video_spider`：`702` 行，`702` 个唯一 URL
*   `9_GDZJ_ZJGZ_00_000000`：`30` 行，`15` 个唯一 URL
*   `9_GDZJ_HYDF_00_000000`：`15` 行，`15` 个唯一 URL
*   `9_GDZJ_ZJGH_00_000000`：`15` 行，`15` 个唯一 URL
*   `9_LN_LNSXWCBGDJ_00_210000`：`12` 行，`12` 个唯一 URL

结论：线上确实存在少量重复写入历史，但规模很小。

### A.4 基准测试方法
为了排除线上数据量较小带来的干扰，额外构造了一张临时基准表，使用与正式表相同的主键设计：

```sql
CREATE TABLE default.bench5 (
    url String,
    site_name LowCardinality(String),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (site_name, url);
```

随后执行以下测试步骤：
1. 关闭后台合并：`SYSTEM STOP MERGES default.bench5`
2. 插入 `10 万` 个唯一 URL
3. 对同一批 URL 连续写入 `3` 个版本，总行数 `30 万`
4. 分别测量：
   *   普通查询耗时
   *   `FINAL` 去重查询耗时
   *   `OPTIMIZE TABLE ... FINAL` 耗时
5. 重新查看合并后的物理行数和查询耗时

### A.5 实测结果
插入耗时：
*   第 1 次写入约 `0.044s`
*   第 2 次写入约 `0.040s`
*   第 3 次写入约 `0.043s`

关闭后台合并后的表状态：

```sql
SELECT
    count() AS active_parts,
    sum(rows) AS part_rows
FROM system.parts
WHERE database='default' AND table='bench5' AND active;
```

结果：
*   `3` 个 active parts
*   `300000` 行物理数据

未合并时的统计查询：

```sql
SELECT
    count() AS rows,
    uniqExact((site_name, url)) AS uniq_keys,
    min(version) AS min_v,
    max(version) AS max_v
FROM default.bench5;
```

结果：
*   总行数：`300000`
*   唯一键数：`100000`
*   版本范围：`1 ~ 3`
*   查询耗时约 `0.054s`

未合并时的 `FINAL` 去重查询：

```sql
SELECT count() AS deduped_rows
FROM default.bench5 FINAL;
```

结果：
*   去重后行数：`100000`
*   查询耗时约 `0.031s`

强制合并：

```sql
OPTIMIZE TABLE default.bench5 FINAL;
```

结果：
*   执行耗时约 `0.043s`

合并后的表状态：
*   active parts：`1`
*   物理行数：`100000`

合并后的普通查询：

```sql
SELECT
    count() AS rows_after_merge,
    uniqExact((site_name, url)) AS uniq_keys_after_merge
FROM default.bench5;
```

结果：
*   行数：`100000`
*   唯一键数：`100000`
*   查询耗时约 `0.021s`

### A.6 结论
本次测试表明：
*   对于 `10 万` 唯一 URL、`3` 倍重复写入的测试规模，ClickHouse 的去重性能表现良好。
*   `FINAL` 查询和 `OPTIMIZE FINAL` 均在几十毫秒量级。
*   在当前平台的数据量级下，`ReplacingMergeTree(version)` 的去重能力不是主要性能瓶颈。
*   当前真正影响是否重复推送 Kafka 的关键，仍然是应用侧是否在写入前做 `site_name + url` 过滤。

### A.7 注意事项
*   `ReplacingMergeTree` 不是“插入前拦截重复”，而是依赖后台合并、`OPTIMIZE FINAL` 或查询时使用 `FINAL` 才体现出去重效果。
*   如果业务查询要求严格只看最新版本，建议：
   *   使用 `FINAL`
   *   或使用 `argMax(..., version)` 方式显式取最新记录
*   对当前爬虫平台而言，应用层先按 `site_name + url` 去重，再使用 ClickHouse 做最终兜底，是更稳妥的设计。

## 附录 B. 本地 Codex 生成与一键部署设计

### B.1 设计目标
为了降低新爬虫接入成本，平台在 dashboard 中增加“AI 生成爬虫”能力。该能力的目标不是直接在页面内执行爬虫，而是完成以下链路：

1.  用户在 dashboard 输入站点需求。
2.  系统调用本机 Codex CLI 生成 `spider` 文件和 `deploy` 文件。
3.  用户确认代码后，在页面上点击部署。
4.  dashboard 将代码写入项目目录。
5.  dashboard 触发 Prefect 中的“部署注册器”任务。
6.  真正的 `deploy_xxx.py` 执行发生在 worker 节点，而不是 dashboard 容器内。

该设计保证了“控制面”和“执行面”职责分离：
*   dashboard 负责收集需求、展示代码、发起请求。
*   worker 负责真正执行部署注册动作。

### B.2 总体架构

```text
浏览器
  -> custom-dashboard
      -> 宿主机 local_codex_helper
          -> 本地 codex exec
      -> 写入 spider/deploy 文件
      -> 调用 Prefect API 创建 flow run
          -> platform-register-deployment
              -> crawler-worker 执行 deploy_xxx.py
                  -> 新 deployment 注册到 Prefect
```

其中：
*   `custom-dashboard` 是控制入口。
*   `local_codex_helper` 是宿主机上的本地辅助服务，专门封装 `codex exec`。
*   `platform-register-deployment` 是运行在 worker 上的管理 deployment，用于执行已有或新生成的 `deploy_*.py`。

### B.3 本地 Codex Helper 的职责
本地 helper 的核心作用是桥接“dashboard 容器”与“宿主机 Codex CLI”。

原因：
*   宿主机上已经存在可用的 `codex` 命令，并且已登录。
*   dashboard 运行在 Docker 容器内，默认无法直接访问宿主机上的 `codex` CLI。
*   将 Codex CLI 和登录态直接塞进容器，运维复杂度较高。

因此采用宿主机常驻 helper 方案：
*   监听本地 HTTP 端口。
*   接收 dashboard 传来的 `system_prompt` 与 `user_prompt`。
*   在宿主机调用 `codex exec`。
*   利用 JSON Schema 约束 Codex 最终输出结构。
*   将 `summary`、`spider_code`、`deploy_code` 返回给 dashboard。

当前实现文件：
*   [local_codex_helper.py](/home/blank/playground/prefect_demo/tools/local_codex_helper.py)
*   [start_local_codex_helper.sh](/home/blank/playground/prefect_demo/tools/start_local_codex_helper.sh)

### B.4 Dashboard 侧生成逻辑
dashboard 后端优先使用本地 helper：

1.  如果配置了 `LOCAL_CODEX_HELPER_URL`
    *   调用本地 helper 的 `/generate`
    *   由宿主机 Codex 生成代码
2.  否则
    *   回退到远程 API 方式

这一策略的好处是：
*   优先利用本地已登录的 Codex CLI。
*   保留远程 API 作为兜底路径。
*   不影响现有部署与运行逻辑。

当前实现位置：
*   [main.py](/home/blank/playground/prefect_demo/dashboard/main.py)

dashboard 前端页面提供两阶段交互：
*   `生成代码`
*   `写入并交给 Worker 部署`

这样做的意义是：
*   用户可以先查看和修改 AI 生成结果。
*   避免模型生成后直接黑盒落盘并部署。

页面实现位置：
*   [index.html](/home/blank/playground/prefect_demo/dashboard/static/index.html)

### B.5 Worker 侧部署注册器
为了确保 dashboard 只负责“发起”，真正的部署注册动作交由 worker 执行，平台新增了一个专用管理 flow：

*   Flow 名称：`平台运维_注册爬虫部署`
*   Deployment 名称：`platform-register-deployment`

它的职责是：
*   接收一个 `deploy_script` 参数
*   在 worker 节点执行对应的 `python spiders/deploy_xxx.py`
*   将新的 deployment 注册到 Prefect

当前实现文件：
*   [platform_ops.py](/home/blank/playground/prefect_demo/spiders/platform_ops.py)
*   [deploy_platform_ops.py](/home/blank/playground/prefect_demo/spiders/deploy_platform_ops.py)

这样可以保证：
*   dashboard 不直接执行部署脚本
*   deployment 注册动作纳入 Prefect 调度和审计范围
*   平台内所有部署操作都能在 Prefect 中看到对应 flow run 记录

### B.6 重新部署旧爬虫
对于已经存在的 deployment，平台增加了页面级“重新部署”能力。

逻辑是：
1.  dashboard 根据 deployment 名推导对应的 `deploy_*.py`
2.  再触发 `platform-register-deployment`
3.  worker 执行该部署脚本
4.  Prefect 中旧 deployment 被重新注册更新

注意：
*   如果只是修改了爬虫逻辑文件，而 worker 通过目录挂载直接读取项目代码，通常只需点击“立即运行”即可跑到新代码。
*   如果修改了 deployment 元信息，例如：
   *   名称
   *   调度周期
   *   tags
   *   description
   *   entrypoint
  则需要点击“重新部署”。

### B.7 运行依赖与网络要求
本地 Codex 方案依赖以下条件：

1.  宿主机已安装并可运行 `codex`
2.  `codex login status` 为可用状态
3.  本地 helper 已启动
4.  dashboard 容器可通过 `host.docker.internal` 访问宿主机 helper

当前 `docker-compose` 中已补充：
*   `LOCAL_CODEX_HELPER_URL`
*   `extra_hosts: host.docker.internal:host-gateway`
*   项目目录挂载到 dashboard 容器，便于写入生成代码

配置位置：
*   [docker-compose.yml](/home/blank/playground/prefect_demo/docker-compose.yml)

### B.8 当前收益
引入本地 Codex 生成链路后，平台具备了以下能力：
*   在页面内输入需求，直接生成爬虫与部署脚本
*   生成逻辑优先使用本机 Codex，而不是必须依赖远程 API
*   生成后的部署仍然通过 worker 执行，符合平台职责分层
*   旧 deployment 也可在页面上一键重新部署

这使得平台从“运行已有爬虫”扩展为“生成、查看、部署、重部署”一体化工作台。
