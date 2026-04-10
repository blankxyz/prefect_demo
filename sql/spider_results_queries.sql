-- Recent rows across all spiders
SELECT
    item_id,
    spider_name,
    flow_run_id,
    source_name,
    item_type,
    url,
    published_at,
    crawled_at
FROM spider_results
ORDER BY item_id DESC
LIMIT 50;

-- Recent runs for a single spider
SELECT
    flow_run_id,
    spider_name,
    COUNT(*) AS item_count,
    MIN(crawled_at) AS first_seen_at,
    MAX(crawled_at) AS last_seen_at
FROM spider_results
WHERE spider_name = 'gdj_video_spider'
GROUP BY flow_run_id, spider_name
ORDER BY last_seen_at DESC;

-- Rows for one flow run
SELECT
    spider_name,
    item_type,
    url,
    published_at,
    data
FROM spider_results
WHERE flow_run_id = 'REPLACE_WITH_FLOW_RUN_ID'
ORDER BY item_id DESC;

-- Extract common fields from JSONB
SELECT
    spider_name,
    flow_run_id,
    item_type,
    data->>'title' AS title,
    data->>'category' AS category,
    data->>'author' AS author,
    data->>'text_content' AS text_content,
    url
FROM spider_results
ORDER BY item_id DESC
LIMIT 100;

-- Filter videos by category
SELECT
    flow_run_id,
    data->>'title' AS title,
    data->>'category' AS category,
    url,
    published_at
FROM spider_results
WHERE item_type = 'video'
  AND data->>'category' = '本土纪录片'
ORDER BY published_at DESC NULLS LAST, item_id DESC;

-- Filter quotes by author
SELECT
    flow_run_id,
    data->>'author' AS author,
    data->>'text_content' AS quote_text,
    url
FROM spider_results
WHERE item_type = 'quote'
  AND data->>'author' = 'Albert Einstein'
ORDER BY item_id DESC;

-- Search a tag inside JSONB array
SELECT
    flow_run_id,
    data->>'title' AS title,
    data->'tags' AS tags,
    url
FROM spider_results
WHERE data->'tags' @> '["inspiration"]'::jsonb
ORDER BY item_id DESC;

-- Count rows by spider and item type
SELECT
    spider_name,
    item_type,
    COUNT(*) AS row_count
FROM spider_results
GROUP BY spider_name, item_type
ORDER BY row_count DESC;

