USE taxi_dw;

-- 创建ADS表（如果不存在）
CREATE TABLE IF NOT EXISTS ads_trip_summary (
    pickup_date DATE,
    PULocationID INT,
    trip_count BIGINT,
    avg_fare_amount DOUBLE,
    avg_tip_amount DOUBLE,
    total_amount_sum DOUBLE
)
COMMENT 'ADS层：每日各区域行程汇总'
STORED AS TEXTFILE;  -- TEXTFILE方便导出为CSV

-- 从DWS表插入数据（使用你优化后的DWS表）
INSERT OVERWRITE TABLE ads_trip_summary
SELECT
    pickup_date,
    PULocationID,
    trip_count,
    avg_fare_amount,
    avg_tip_amount,
    total_amount_sum
FROM dws_trip_agg_location_day;