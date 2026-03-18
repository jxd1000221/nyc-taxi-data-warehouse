-- 切换到 taxi_dw 数据库
USE taxi_dw;

-- 设置相关参数（优化MapReduce）
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;                -- 开启并行执行
SET hive.vectorized.execution.enabled=true; -- 开启向量化

-- ------------------- 第一步：创建DWS表 -------------------
-- 按上车区域和日期聚合，存储每日各区域的订单量、平均车费等
CREATE TABLE IF NOT EXISTS dws_trip_agg_location_day (
    PULocationID INT,
    trip_count BIGINT,
    avg_fare_amount DOUBLE,
    avg_tip_amount DOUBLE,
    avg_trip_distance DOUBLE,
    total_amount_sum DOUBLE
)
COMMENT '按区域和日期聚合的行程统计'
PARTITIONED BY (pickup_date DATE)   -- 按日期分区
STORED AS ORC;

-- ------------------- 第二步：正常聚合 -------------------
-- 先尝试直接聚合，观察性能
-- INSERT OVERWRITE TABLE dws_trip_agg_location_day PARTITION (pickup_date)
-- SELECT
--     PULocationID,
--     COUNT(*) AS trip_count,
--     AVG(fare_amount) AS avg_fare_amount,
--     AVG(tip_amount) AS avg_tip_amount,
--     AVG(trip_distance) AS avg_trip_distance,
--     SUM(total_amount) AS total_amount_sum,
--     pickup_date
-- FROM dwd_taxi_tripdata
-- GROUP BY PULocationID, pickup_date;

-- ------------------- 第三步：优化数据倾斜的聚合（加盐两阶段聚合） -------------------
-- 原理：先给热点key加随机前缀，分散到多个reduce，再去掉前缀聚合
-- 注意：仅当某些区域数据量极大时才需要此优化，这里作为示例演示

-- 第1阶段：加盐聚合（将key加上随机后缀，分散负载）
WITH salted AS (
    SELECT
        -- 给PULocationID加上随机数前缀（0-9），将数据分散到10个桶
        CONCAT(CAST(PULocationID AS STRING), '_', CAST(FLOOR(RAND() * 10) AS STRING)) AS salted_key,
        fare_amount,
        tip_amount,
        trip_distance,
        total_amount,
        pickup_date
    FROM dwd_taxi_tripdata
),
salted_agg AS (
    SELECT
        salted_key,
        pickup_date,
        COUNT(*) AS partial_count,
        SUM(fare_amount) AS partial_fare_sum,
        SUM(tip_amount) AS partial_tip_sum,
        SUM(trip_distance) AS partial_distance_sum,
        SUM(total_amount) AS partial_total_sum
    FROM salted
    GROUP BY salted_key, pickup_date
)
-- 第2阶段：去掉前缀，最终聚合
INSERT OVERWRITE TABLE dws_trip_agg_location_day PARTITION (pickup_date)
SELECT
    -- 从salted_key中提取原始PULocationID
    CAST(SPLIT(salted_key, '_')[0] AS INT) AS PULocationID,
    SUM(partial_count) AS trip_count,
    SUM(partial_fare_sum) / SUM(partial_count) AS avg_fare_amount,
    SUM(partial_tip_sum) / SUM(partial_count) AS avg_tip_amount,
    SUM(partial_distance_sum) / SUM(partial_count) AS avg_trip_distance,
    SUM(partial_total_sum) AS total_amount_sum,
    pickup_date
FROM salted_agg
GROUP BY CAST(SPLIT(salted_key, '_')[0] AS INT), pickup_date;