-- Ingest data
%scala
spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net" , "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net " , "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net" , "f3905ff9-16d4-43ac-9011-842b661d556d")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net" , "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net" , "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

--Drop tables for indepotent
%scala
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/hotel_weather_bronze", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/expedia_bronze", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/hotel_weather_silver", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/expedia_silver", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/top_10_hotels_with_max_abs_temp_diff_gold", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/top_10_busy_hotels_for_each_month_gold", true)
dbutils.fs.rm("dbfs:/user/hive/warehouse/spark_sql_database.db/tmpr_trend_and_avg_temp_gold", true)

--Create database for tables
CREATE DATABASE IF NOT EXISTS spark_sql_database;
USE spark_sql_database;

--Create bronze weather table
DROP TABLE IF EXISTS hotel_weather_bronze;
CREATE TABLE IF NOT EXISTS hotel_weather_bronze (
 address STRING,
 avg_tmpr_c DOUBLE,
 avg_tmpr_f DOUBLE,
 city STRING,
 country STRING,
 geoHash STRING,
 id STRING,
 latitude DOUBLE,
 longitude DOUBLE,
 name STRING,
 wthr_date STRING,
 year STRING,
 month STRING,
 day STRING
)
USING PARQUET
OPTIONS (path 'abfss://m07sparksql@bd201stacc.dfs.core.windows.net/hotel-weather');

--Show 10 rwos
SELECT * FROM hotel_weather_bronze LIMIT 10;

--Create bronze expedia table
DROP TABLE IF EXISTS expedia_bronze;
CREATE TABLE IF NOT EXISTS expedia_bronze (
 id BIGINT,
 date_time STRING,
 site_name INT,
 posa_continent INT,
 user_location_country INT,
 user_location_region INT,
 user_location_city INT,
 orig_destination_distance DOUBLE,
 user_id INT,
 is_mobile INT,
 is_package INT,
 channel INT,
 srch_ci STRING,
 srch_co STRING,
 srch_adults_cnt INT,
 srch_children_cnt INT,
 srch_rm_cnt INT,
 srch_destination_id INT,
 srch_destination_type_id INT,
 hotel_id BIGINT
)
USING AVRO
OPTIONS (path 'abfss://m07sparksql@bd201stacc.dfs.core.windows.net/expedia');

--Show 10 rwos
SELECT * FROM expedia_bronze LIMIT 10;

--Create silver weather table
DROP TABLE IF EXISTS hotel_weather_silver;
CREATE TABLE IF NOT EXISTS hotel_weather_silver
USING DELTA
PARTITIONED BY (year, month, day)
AS (
  SELECT
    address,
    avg_tmpr_c,
    avg_tmpr_f,
    city,
    country,
    geoHash,
    CAST(id AS bigint),
    latitude,
    longitude,
    name,
    CAST(wthr_date AS date) AS wthr_date,
    year,
    month,
    day
 FROM hotel_weather_bronze
);

--Show 10 rwos
SELECT * FROM hotel_weather_silver LIMIT 10;

--Create silver expedia table
DROP TABLE IF EXISTS expedia_silver;
CREATE TABLE IF NOT EXISTS expedia_silver
USING DELTA
AS (
  SELECT
    id,
    CAST(date_time as date) AS date_time,
    site_name,
    posa_continent,
    user_location_country,
    user_location_region,
    user_location_city,
    orig_destination_distance,
    user_id,
    is_mobile,
    is_package,
    channel,
    CAST(srch_ci AS date) AS srch_ci,
    CAST(srch_co AS date) AS srch_co,
    srch_adults_cnt,
    srch_children_cnt,
    srch_rm_cnt,
    srch_destination_id,
    srch_destination_type_id,
    hotel_id
 FROM expedia_bronze
);

--Show 10 rwos
SELECT * FROM expedia_silver LIMIT 10;

--Create table 'Top 10 hotels with max absolute temperature difference by month'
DROP TABLE IF EXISTS top_10_hotels_with_max_abs_temp_diff_gold;
CREATE TABLE IF NOT EXISTS top_10_hotels_with_max_abs_temp_diff_gold AS (
SELECT
  id,
  month,
  MAX(avg_tmpr_c) - MIN(avg_tmpr_c) AS max_abs_temp_diff
FROM hotel_weather_silver
GROUP BY id, month
ORDER BY max_abs_temp_diff DESC
LIMIT 10);

--Show top 10 hotels with max absolute temperature difference by month
SELECT * FROM top_10_hotels_with_max_abs_temp_diff_gold;

--Create table 'Top 10 busy (e.g., with the biggest visits count) hotels for each month'
DROP TABLE IF EXISTS top_10_busy_hotels_for_each_month_gold;
CREATE TABLE IF NOT EXISTS top_10_busy_hotels_for_each_month_gold AS (
SELECT
  hotel_id,
  year(booking_day) AS year,
  month(booking_day) AS month,
  count(booking_day) AS booking_days_count
FROM (
  SELECT
    hotel_id,
    EXPLODE(
    CASE
      WHEN datediff(srch_co, srch_ci) >= 1
      THEN sequence(srch_ci, srch_co, INTERVAL 1 day)
      ELSE array(srch_ci)
    END) AS booking_day
  FROM expedia_silver
)
GROUP BY hotel_id, year, month
ORDER BY booking_days_count DESC
LIMIT 10);

--Show top 10 busy hotels for each month
SELECT * FROM top_10_busy_hotels_for_each_month_gold;

--Create weather trend table
DROP TABLE IF EXISTS tmpr_trend_and_avg_temp_gold;
CREATE TABLE IF NOT EXISTS tmpr_trend_and_avg_temp_gold AS (
WITH booking_extended_stay AS (
  SELECT
    hotel_id,
    id AS booking_id,
    srch_ci,
    srch_co,
    explode(
       CASE
        WHEN datediff(srch_co, srch_ci) >= 1
        THEN sequence(srch_ci, srch_co, INTERVAL 1 day)
        ELSE array(srch_ci)
       END) AS booking_day
  FROM expedia_silver
  WHERE datediff(srch_co, srch_ci) > 7
),
joined_tables AS (
  SELECT
    *,
    first_value(hws.avg_tmpr_c) OVER (PARTITION BY hotel_id, booking_id ORDER BY booking_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_day_tmpr_c,
    last_value(hws.avg_tmpr_c) OVER (PARTITION BY hotel_id, booking_id ORDER BY booking_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day_tmpr_c,
    avg(hws.avg_tmpr_c) OVER (PARTITION BY hotel_id, booking_id) AS booking_avg_tmpr_c
  FROM booking_extended_stay AS bes
  INNER JOIN hotel_weather_silver AS hws
  ON bes.hotel_id = hws.id AND bes.booking_day = hws.wthr_date
),
check_start_and_end_date AS (
  SELECT
    *,
    count(*) OVER(PARTITION BY hotel_id, booking_id) AS rows_count
  FROM joined_tables
  WHERE srch_ci == wthr_date OR srch_co == wthr_date
)
SELECT DISTINCT
  hotel_id,
  booking_id,
  srch_ci,
  srch_co,
  booking_avg_tmpr_c,
  CASE
    WHEN rows_count == 2
    THEN last_day_tmpr_c - first_day_tmpr_c
    ELSE "Start or end date not found for weather trend calculation"
    END AS booking_weather_trend_tmpr_c
FROM check_start_and_end_date
);

--Show 10 rows from weather trend table
SELECT * FROM tmpr_trend_and_avg_temp_gold LIMIT 10;

--Just for chart "max_avg_tmpr_c_for_bookings"
SELECT
  hotel_id,
  booking_id,
  MAX(booking_avg_tmpr_c) as max_avg_tmpr_c
FROM tmpr_trend_and_avg_temp_gold
GROUP BY hotel_id, booking_id
ORDER BY max_avg_tmpr_c DESC
LIMIT 10;

--Save data marts to ADLS
%scala
spark.conf.set("fs.azure.account.key.stsparkbasicwesteurope.dfs.core.windows.net", "arh+EEs7+t+Pw1G22RzfuQGCYbKQpGz09vUUdT2XGJ/VQ3i/TkpTyFuXL/NF9ZbZA62Zvo3jXV9uOAM8amJnPg==");

val df1 = spark.read.format("delta").load("dbfs:/user/hive/warehouse/spark_sql_database.db/top_10_hotels_with_max_abs_temp_diff_gold")
val df2 = spark.read.format("delta").load("dbfs:/user/hive/warehouse/spark_sql_database.db/top_10_busy_hotels_for_each_month_gold")
val df3 = spark.read.format("delta").load("dbfs:/user/hive/warehouse/spark_sql_database.db/tmpr_trend_and_avg_temp_gold")

df1.write.mode("overwrite").parquet("abfss://data@stsparkbasicwesteurope.dfs.core.windows.net/sql/top_10_hotels_with_max_abs_temp_diff_gold.parquet");
df2.write.mode("overwrite").parquet("abfss://data@stsparkbasicwesteurope.dfs.core.windows.net/sql/top_10_busy_hotels_for_each_month_gold.parquet");
df3.write.mode("overwrite").parquet("abfss://data@stsparkbasicwesteurope.dfs.core.windows.net/sql/tmpr_trend_and_avg_temp_gold.parquet");