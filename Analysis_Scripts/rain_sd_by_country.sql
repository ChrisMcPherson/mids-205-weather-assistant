-- calculate deltas by retrieval date
-- delta_2_1 means diff between temp of day 2 and day 1 (COALESCE(rain_2,0) - COALESCE(rain_1)
DROP TABLE IF EXISTS city_deltas;
CREATE TEMPORARY TABLE city_deltas AS
select
  city_id,
  city_name,
  country_code,
  retrieval_date,
  (COALESCE(rain_2,0) - COALESCE(rain_1,0)) as delta_2_1,
  (COALESCE(rain_3,0) - COALESCE(rain_1,0)) as delta_3_1,
  (COALESCE(rain_4,0) - COALESCE(rain_1,0)) as delta_4_1,
  (COALESCE(rain_5,0) - COALESCE(rain_1,0)) as delta_5_1,
  (COALESCE(rain_6,0) - COALESCE(rain_1,0)) as delta_6_1,
  (COALESCE(rain_7,0) - COALESCE(rain_1,0)) as delta_7_1
from forecast_weather_flat_stream
group by 1,2,3,4, rain_1, rain_2, rain_3, rain_4, rain_5, rain_6, rain_7
order by city_id;


-- calulate sd for each city by this formula:
-- sd = square root ( SUM(deltas ^ 2)/ N)
-- we round sd to 2 dp
DROP TABLE IF EXISTS city_sd_rain;
CREATE TABLE city_sd_rain AS
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
    SUM(delta_2_1 ^ 2) as deltas_sq_2_1,
    SUM(delta_3_1 ^ 2) as deltas_sq_3_1,
    SUM(delta_4_1 ^ 2) as deltas_sq_4_1,
    SUM(delta_5_1 ^ 2) as deltas_sq_5_1,
    SUM(delta_6_1 ^ 2) as deltas_sq_6_1,
    SUM(delta_7_1 ^ 2) as deltas_sq_7_1
  FROM
    city_deltas
  GROUP BY 1,2,3
   ) AS d1
JOIN
  (SELECT
    city_id,
    COUNT(DISTINCT retrieval_date) as date_count
  FROM
    city_deltas
  GROUP BY 1
  ) AS d2
ON
  d1.city_id = d2.city_id
GROUP BY d1.city_id, d1.city_name, d1.country_code, d1.deltas_sq_2_1,d1.deltas_sq_3_1,d1.deltas_sq_4_1,d1.deltas_sq_5_1,d1.deltas_sq_6_1,d1.deltas_sq_7_1, d2.date_count ;



 