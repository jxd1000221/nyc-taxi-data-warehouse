-- 1. 切换到 taxi_dw 数据库
USE taxi_dw;

-- 2. 设置动态分区（允许Hive根据数据自动创建分区目录）
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 3. 创建DWD表
--    DWD层存储清洗后的明细数据，并添加时间维度字段
CREATE TABLE IF NOT EXISTS dwd_taxi_tripdata (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    -- 新增的时间维度字段，方便后续按天/小时/星期分析
    pickup_date DATE,
    pickup_hour INT,
    pickup_weekday STRING
)
COMMENT '清洗后的出租车数据，包含时间维度'
PARTITIONED BY (pickup_date DATE)   -- 按上车日期分区，查询更快
STORED AS ORC;                      -- ORC格式比TEXTFILE查询性能更好

-- 4. 从ODS表清洗数据并插入DWD表
--    这里做了几件事：
--       - 过滤掉明显不合理的数据（乘客数<=0、距离<=0、车费<=0等）
--       - 将DOUBLE类型转换为INT（如passenger_count）
--       - 从时间戳中提取日期、小时、星期几
INSERT OVERWRITE TABLE dwd_taxi_tripdata PARTITION (pickup_date)
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    CAST(passenger_count AS INT) AS passenger_count,   -- 转为整数
    trip_distance,
    CAST(RatecodeID AS INT) AS RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee,
    cbd_congestion_fee,
    -- 提取上车日期
    TO_DATE(tpep_pickup_datetime) AS pickup_date,
    -- 提取上车小时（0-23）
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    -- 将星期几数字转换为英文名称（方便阅读）
    CASE 
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 1 THEN 'Sunday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 2 THEN 'Monday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 3 THEN 'Tuesday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 4 THEN 'Wednesday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 5 THEN 'Thursday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 6 THEN 'Friday'
        WHEN DAYOFWEEK(tpep_pickup_datetime) = 7 THEN 'Saturday'
    END AS pickup_weekday
FROM ods_taxi_tripdata
WHERE 
    passenger_count > 0               -- 乘客数必须为正
    AND trip_distance > 0              -- 行程距离必须为正
    AND fare_amount > 0                 -- 车费必须为正
    AND total_amount > 0                -- 总金额必须为正
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL;