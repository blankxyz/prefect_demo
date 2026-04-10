-- 创建数据库表
CREATE TABLE IF NOT EXISTS crawler_data (
    url String,
    site_name LowCardinality(String),
    raw_json JSON,                  -- 使用原生 JSON 类型 
    crawl_time DateTime64(3),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (site_name, url);

-- 异步攒批配置已通过 async_insert.xml 挂载到 ClickHouse，无需 ALTER USER
