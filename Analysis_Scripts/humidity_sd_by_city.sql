-- calculate deltas by retrieval date
-- delta_2_1 means diff between temp of day 2 and day 1 (humidity_2 - humidity_1)
DROP TABLE IF EXISTS city_deltas;
CREATE TEMPORARY TABLE city_deltas AS
select
  city_id,
  city_name,
  country_code,
  retrieval_date,
  (humidity_2 - humidity_1) AS delta_2_1,
  (humidity_3 - humidity_1) AS delta_3_1,
  (humidity_4 - humidity_1) AS delta_4_1,
  (humidity_5 - humidity_1) AS delta_5_1,
  (humidity_6 - humidity_1) AS delta_6_1,
  (humidity_7 - humidity_1) AS delta_7_1
from forecast_weather_flat
group by 1,2,3,4, humidity_1, humidity_2, humidity_3, humidity_4, humidity_5, humidity_6, humidity_7
order by city_id;


-- calulate sd for each city by this formula:
-- sd = square root ( SUM(deltas ^ 2)/ N)
-- we round sd to 2 dp
DROP TABLE IF EXISTS city_sd_humidity;
CREATE TABLE city_sd_humidity AS
SELECT
  d1.city_id AS city_id,
  d1.city_name AS city_name,
  d1.country_code AS country_code,
  round(CAST(|/(d1.deltas_sq_2_1/ d2.date_count) AS NUMERIC), 2) AS sd_2,
  round(CAST(|/(d1.deltas_sq_3_1/ d2.date_count) AS NUMERIC), 2) AS sd_3,
  round(CAST(|/(d1.deltas_sq_4_1/ d2.date_count) AS NUMERIC), 2) AS sd_4,
  round(CAST(|/(d1.deltas_sq_5_1/ d2.date_count) AS NUMERIC), 2) AS sd_5,
  round(CAST(|/(d1.deltas_sq_6_1/ d2.date_count) AS NUMERIC), 2) AS sd_6,
  round(CAST(|/(d1.deltas_sq_7_1/ d2.date_count) AS NUMERIC), 2) AS sd_7
FROM
  (
  SELECT
    city_id,
    city_name,
    country_code,
    SUM(delta_2_1 ^ 2) AS deltas_sq_2_1,
    SUM(delta_3_1 ^ 2) AS deltas_sq_3_1,
    SUM(delta_4_1 ^ 2) AS deltas_sq_4_1,
    SUM(delta_5_1 ^ 2) AS deltas_sq_5_1,
    SUM(delta_6_1 ^ 2) AS deltas_sq_6_1,
    SUM(delta_7_1 ^ 2) AS deltas_sq_7_1
  FROM
    city_deltas
  GROUP BY 1,2,3
   ) AS d1
JOIN
  (SELECT
    city_id,
    COUNT(DISTINCT retrieval_date) AS date_count
  FROM
    city_deltas
  GROUP BY 1
  ) AS d2
ON
  d1.city_id = d2.city_id
GROUP BY d1.city_id, d1.city_name, d1.country_code, d1.deltas_sq_2_1,d1.deltas_sq_3_1,d1.deltas_sq_4_1,d1.deltas_sq_5_1,d1.deltas_sq_6_1,d1.deltas_sq_7_1, d2.date_count;