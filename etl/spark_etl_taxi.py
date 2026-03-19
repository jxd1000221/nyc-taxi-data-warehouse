#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, hour, dayofweek
from pyspark.sql.types import IntegerType, DoubleType, StringType

# 1. 启动 Spark
spark = SparkSession.builder \
    .appName("NYC_Taxi_ETL") \
    .enableHiveSupport() \
    .getOrCreate()

# 配置动态分区和类型兼容性
spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
# Spark 3.x 写入 Hive 建议开启 LEGACY 模式，防止因微小的精度差异报错
spark.sql("SET spark.sql.storeAssignmentPolicy=LEGACY")

print(">>> 正在从 ODS 读取数据...")
df = spark.table("taxi_dw.ods_taxi_tripdata")

# ==================================================
# ==================================================
# 2. 类型转换 (确保每一行都赋值回 df)
# ==================================================
print(">>> 正在进行强制类型转换...")
# 我们用一个新的变量名 df_casted，确保不被干扰
df_casted = df.select(
    col("vendorid").cast("int"),
    to_timestamp(col("tpep_pickup_datetime")).alias("tpep_pickup_datetime"),
    to_timestamp(col("tpep_dropoff_datetime")).alias("tpep_dropoff_datetime"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("ratecodeid").cast("int"),
    col("store_and_fwd_flag").cast("string"),
    col("pulocationid").cast("int"),
    col("dolocationid").cast("int"),
    col("payment_type").cast("int"),
    col("fare_amount").cast("double"),
    col("extra").cast("double"),
    col("mta_tax").cast("double"),
    col("tip_amount").cast("double"),
    col("tolls_amount").cast("double"),
    col("improvement_surcharge").cast("double"),
    col("total_amount").cast("double"),
    col("congestion_surcharge").cast("double"),
    col("airport_fee").cast("double"),
    col("cbd_congestion_fee").cast("double")
)

# =========================
# 3. 数据清洗
# =========================
df_cleaned = df_casted.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    col("tpep_pickup_datetime").isNotNull()
)

# =========================
# 4. 时间维度 (严格匹配 Hive 类型)
# =========================
df_final = df_cleaned \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")).cast("int")) \
    .withColumn("pickup_weekday", dayofweek(col("tpep_pickup_datetime")).cast("string")) \
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

# 【关键诊断步】打印 Schema，运行的时候盯着屏幕看这里！
print(">>> 写入前的 Dataframe 结构如下：")
df_final.printSchema()

# ==================================================
# 5. 写入 DWD (加上强制 Legacy 配置)
# ==================================================
# 这行配置非常重要，它允许 Spark 尝试更宽松的类型转换
spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")

print(">>> 正在写入 DWD 表...")
df_final.select(
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "ratecodeid",
    "store_and_fwd_flag", "pulocationid", "dolocationid",
    "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee", "pickup_hour", "pickup_weekday",
    "pickup_date"
).write \
.mode("overwrite") \
.insertInto("taxi_dw.dwd_taxi_tripdata")

print(">>> 写入成功！")
# =========================
# 6. 验证
# =========================
res = spark.sql("SELECT COUNT(*) FROM taxi_dw.dwd_taxi_tripdata").collect()
print(f">>> DWD 总行数: {res[0][0]}")

spark.stop()