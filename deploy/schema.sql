-- ============================================================================
-- ClickHouse TimeSeries Schema for Prometheus
-- ============================================================================
CREATE DATABASE IF NOT EXISTS timeseries_db;

-- ============================================================================
-- RAW DATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_data_table
(
    `id` UUID,
    `timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `value` Float64 CODEC(Gorilla, ZSTD(1))
)
ENGINE = ReplacingMergeTree(timestamp)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 14 DAY DELETE
SETTINGS 
    index_granularity = 8192,
    compress_marks = true,
    compress_primary_key = true,
    -- Optimized compression for time series data
    min_compress_block_size = 65536,
    max_compress_block_size = 1048576;

-- ============================================================================
-- 1-MINUTE AGGREGATION TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_1m_table
(
    `id` UUID,
    `timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `sum_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `cnt` UInt64 CODEC(T64, ZSTD(1)),
    `min_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `max_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `value` Float64 ALIAS (sum_val / NULLIF(cnt, 0))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS 
    index_granularity = 8192,
    compress_marks = true,
    compress_primary_key = true,
    min_compress_block_size = 65536,
    max_compress_block_size = 1048576;

-- Materialized view: Downsample from raw data to 1-minute buckets
CREATE MATERIALIZED VIEW IF NOT EXISTS timeseries_db.mv_downsample_1m
TO timeseries_db.timeseries_1m_table
AS
SELECT
    id,
    toStartOfMinute(timestamp) AS timestamp,
    sum(value) AS sum_val,
    count() AS cnt,
    min(value) AS min_val,
    max(value) AS max_val
FROM timeseries_db.timeseries_data_table
GROUP BY id, timestamp;

-- ============================================================================
-- 5-MINUTE AGGREGATION TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_5m_table
(
    `id` UUID,
    `timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `sum_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `cnt` UInt64 CODEC(T64, ZSTD(1)),
    `min_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `max_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `value` Float64 ALIAS (sum_val / NULLIF(cnt, 0))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 1 YEAR DELETE
SETTINGS 
    index_granularity = 8192,
    compress_marks = true,
    compress_primary_key = true,
    min_compress_block_size = 65536,
    max_compress_block_size = 1048576;

-- Materialized view: CASCADE from 1m table
CREATE MATERIALIZED VIEW IF NOT EXISTS timeseries_db.mv_downsample_5m
TO timeseries_db.timeseries_5m_table
AS
SELECT
    id,
    toStartOfFiveMinutes(timestamp) AS timestamp,
    sum(sum_val) AS sum_val,
    sum(cnt) AS cnt,
    min(min_val) AS min_val,
    max(max_val) AS max_val
FROM timeseries_db.timeseries_1m_table
GROUP BY id, timestamp;

-- ============================================================================
-- 1-HOUR AGGREGATION TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_1h_table
(
    `id` UUID,
    `timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `sum_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `cnt` UInt64 CODEC(T64, ZSTD(1)),
    `min_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `max_val` Float64 CODEC(Gorilla, ZSTD(1)),
    `value` Float64 ALIAS (sum_val / NULLIF(cnt, 0))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (id, timestamp)
TTL timestamp + INTERVAL 5 YEAR DELETE
SETTINGS 
    index_granularity = 8192,
    compress_marks = true,
    compress_primary_key = true,
    min_compress_block_size = 65536,
    max_compress_block_size = 1048576;

-- Materialized view: CASCADE from 5m table
CREATE MATERIALIZED VIEW IF NOT EXISTS timeseries_db.mv_downsample_1h
TO timeseries_db.timeseries_1h_table
AS
SELECT
    id,
    toStartOfHour(timestamp) AS timestamp,
    sum(sum_val) AS sum_val,
    sum(cnt) AS cnt,
    min(min_val) AS min_val,
    max(max_val) AS max_val
FROM timeseries_db.timeseries_5m_table
GROUP BY id, timestamp;

-- ============================================================================
-- TAGS/LABELS TABLE
-- ============================================================================
-- Engine: AggregatingMergeTree is required because:
-- 1. min_time and max_time use SimpleAggregateFunction type
-- 2. This is required by aggregate_min_time_and_max_time = true (default)
-- 3. TimeSeries engine will write aggregating inserts to update these columns
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_tags_table
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    -- tags Map
    `tags` Map(LowCardinality(String), String),
    -- all_tags Map WILL contain all labels (EPHEMERAL - only for id calculation)
    `all_tags` Map(String, String) EPHEMERAL,
    `labels_hash` UInt64 MATERIALIZED cityHash64(
        arrayStringConcat(
            arraySort(arrayMap((k,v) -> concat(k, '=', v), mapKeys(tags), mapValues(tags))),
            ','
        )
    ),
    
    -- Time range columns - automatically maintained by TimeSeries engine
    -- Using SimpleAggregateFunction as required by aggregate_min_time_and_max_time setting
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
ORDER BY (metric_name, id)
SETTINGS 
    index_granularity = 8192,
    allow_nullable_key = 1;

-- Indexes
ALTER TABLE timeseries_db.timeseries_tags_table
    ADD INDEX IF NOT EXISTS idx_id id TYPE bloom_filter(0.01) GRANULARITY 4;

ALTER TABLE timeseries_db.timeseries_tags_table
    ADD INDEX IF NOT EXISTS idx_labels_hash labels_hash TYPE set(1000000) GRANULARITY 4;

ALTER TABLE timeseries_db.timeseries_tags_table
    ADD INDEX IF NOT EXISTS idx_metric_name metric_name TYPE bloom_filter(0.001) GRANULARITY 1;

ALTER TABLE timeseries_db.timeseries_tags_table
    ADD INDEX IF NOT EXISTS idx_tags_keys mapKeys(tags) TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE timeseries_db.timeseries_tags_table
    ADD INDEX IF NOT EXISTS idx_tags_values mapValues(tags) TYPE bloom_filter(0.01) GRANULARITY 1;

-- ============================================================================
-- Create addtional indexes on on frequently queried labels if needed, like:
-- ALTER TABLE timeseries_db.timeseries_tags_table
--    ADD INDEX IF NOT EXISTS idx_job tags['job'] TYPE bloom_filter(0.01) GRANULARITY 1;
-- ============================================================================

-- ============================================================================
-- METRICS METADATA TABLE
-- ============================================================================
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_metrics_table
(
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name;

-- ============================================================================
-- TIMESERIES ENGINE TABLES
-- ============================================================================
-- Using built-in min_time/max_time tracking with aggregation
CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_raw
ENGINE = TimeSeries
SETTINGS 
    store_min_time_and_max_time = true,           -- Store min/max time per series
    filter_by_min_time_and_max_time = true,       -- Use for query optimization
    aggregate_min_time_and_max_time = true,       -- Use SimpleAggregateFunction (default)
    tags_to_columns = {}
DATA timeseries_db.timeseries_data_table
TAGS timeseries_db.timeseries_tags_table
METRICS timeseries_db.timeseries_metrics_table;

CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_1m
ENGINE = TimeSeries
SETTINGS 
    store_min_time_and_max_time = false,
    filter_by_min_time_and_max_time = false,
    aggregate_min_time_and_max_time = true,       -- Use SimpleAggregateFunction (default)
    tags_to_columns = {}
DATA timeseries_db.timeseries_1m_table
TAGS timeseries_db.timeseries_tags_table
METRICS timeseries_db.timeseries_metrics_table;

CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_5m
ENGINE = TimeSeries
SETTINGS 
    store_min_time_and_max_time = false,
    filter_by_min_time_and_max_time = false,
    aggregate_min_time_and_max_time = true,       -- Use SimpleAggregateFunction (default)
    tags_to_columns = {}
DATA timeseries_db.timeseries_5m_table
TAGS timeseries_db.timeseries_tags_table
METRICS timeseries_db.timeseries_metrics_table;

CREATE TABLE IF NOT EXISTS timeseries_db.timeseries_1h
ENGINE = TimeSeries
SETTINGS 
    store_min_time_and_max_time = false,
    filter_by_min_time_and_max_time = false,
    aggregate_min_time_and_max_time = true,       -- Use SimpleAggregateFunction (default)
    tags_to_columns = {}
DATA timeseries_db.timeseries_1h_table
TAGS timeseries_db.timeseries_tags_table
METRICS timeseries_db.timeseries_metrics_table;

-- ============================================================================
-- CARDINALITY TRACKING
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS timeseries_db.mv_cardinality_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (metric_name, hour)
AS
SELECT
    t.metric_name,
    toStartOfHour(d.timestamp) AS hour,
    uniqExact(d.id) AS unique_series,
    count() AS data_points
FROM timeseries_db.timeseries_data_table d
INNER JOIN timeseries_db.timeseries_tags_table t ON d.id = t.id
GROUP BY t.metric_name, hour;

-- ============================================================================
-- HELPER VIEWS
-- ============================================================================
CREATE VIEW IF NOT EXISTS timeseries_db.v_series_list AS
SELECT
    id, 
    metric_name, 
    tags
FROM timeseries_db.timeseries_tags_table;