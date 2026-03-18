# NYC Taxi Data Warehouse Project

## 📌 项目概述
本项目基于纽约市出租车与礼车委员会（TLC）公开的2025年1月黄色出租车行程数据，构建了一套完整的离线数据仓库。项目涵盖了从数据导入、清洗、建模、性能优化到可视化的全流程，旨在通过真实数据展示大数据开发的工程实践与业务分析能力。

 **数据规模**：约347万条原始行程记录
 **数据来源**：[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
 **项目周期**：2026年3月
-**技术栈**：`Hadoop (HDFS)` · `Hive` · `Spark SQL` · `Airflow` · `Tableau Public`
## 🏗️ 数仓分层架构
采用经典的四层数仓模型，确保数据流转清晰、易于维护：
分层	           表名	                                主要做了什么
ODS	        ods_taxi_tripdata	       原始数据直接放 HDFS，用外部表   指向文件，方便以后回溯。
DWD	        dwd_taxi_tripdata	       清洗数据：过滤掉乘客数≤0、距离≤0、车费≤0 这些明显不对的记录，顺便把时间戳拆成日期、小时、星期几，方便后面分析。
DWS	        dws_trip_agg_location_day  按区域和日期做轻度汇总，统计每天每个区域的订单量、平均车费、平均小费这些。
ADS	        ads_trip_summary	       最后给 Tableau 用的数据，直接从 DWS 层拿过来，没做太复杂的加工。
## 🛠️ 核心功能与优化
### 1. 数据清洗与预处理
- **过滤脏数据**：剔除乘客数≤0、行程距离≤0、车费≤0等不合理记录，清洗后数据量约为281万条（原始数据的81%）。
- **维度扩展**：从时间戳中提取`pickup_date`、`pickup_hour`、`pickup_weekday`，方便后续按时间维度分析。
### 2. 性能优化：解决数据倾斜
在DWS层按区域聚合时，发现部分热点区域（如机场）数据量远高于其他区域，导致任务执行缓慢。
- **问题诊断**：通过观察MapReduce日志，确认存在数据倾斜。
- **优化方案**：采用**加盐两阶段聚合**策略。
    1. **第一阶段**：给区域ID添加随机前缀（0-9），将数据分散到多个临时key上进行局部聚合。
    2. **第二阶段**：去掉前缀，对局部聚合结果进行全局聚合。
- **优化效果**：
  | 聚合方式      | 执行时间 |
  | 普通GROUP BY  | 78秒 |
  | 加盐两阶段聚合 | **44秒** |
  **性能提升约43.6%**，有效解决了数据倾斜问题。
## 📊 可视化分析成果
使用 **Tableau Public** 制作交互式仪表盘，直观展示分析结果。
👉 **[点击此处查看在线仪表盘](https://public.tableau.com/views/dashboard_png/1_1?:language=zh-CN&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**

仪表盘包含以下核心图表：
1. **区域订单分布地图**：通过颜色深浅展示纽约市各区域订单量，并关联区域名称，直观识别热点区域。
2. **热门区域订单量排行**：条形图展示订单量前10的区域，支持点击筛选联动。
3. **每日订单量趋势**：折线图展示整个1月份的订单量变化，可观察工作日与周末的差异。
4. **平均车费 vs 平均小费散点图**：点的大小代表订单量，揭示车费与小费的正相关关系，并定位高价值区域。

仪表盘截图位于 `visualization/dashboard.png`
📂 项目结构
text
nyc-taxi-data-warehouse/
├── README.md
├── .gitignore
├── hive/
│   ├── ods/01_create_ods_table_csv.sql
│   ├── dwd/02_clean_and_insert_dwd.sql
│   ├── dws/03_dws_aggregation.sql
│   └── ads/04_ads_summary.sql
├── docs/
│   └── optimization.md          # 优化过程随手记的
├── visualization/
│   └── dashboard.png            # Tableau 截图
├── airflow/                      # 以后加调度
└── spark/                        # 以后加 Spark 版本