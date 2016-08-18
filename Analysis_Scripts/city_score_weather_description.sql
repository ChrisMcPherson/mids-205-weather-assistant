DROP TABLE IF EXISTS city_weather_description_match;
CREATE TEMPORARY TABLE city_weather_description_match AS
SELECT
  city_id,
  retrieval_date,
  CASE WHEN weather_description_1 LIKE weather_description_2 THEN 1 ELSE 0 END AS weather_description_2_match,
  CASE WHEN weather_description_1 LIKE weather_description_3 THEN 1 ELSE 0 END AS weather_description_3_match,
  CASE WHEN weather_description_1 LIKE weather_description_4 THEN 1 ELSE 0 END AS weather_description_4_match,
  CASE WHEN weather_description_1 LIKE weather_description_5 THEN 1 ELSE 0 END AS weather_description_5_match,
  CASE WHEN weather_description_1 LIKE weather_description_6 THEN 1 ELSE 0 END AS weather_description_6_match,
  CASE WHEN weather_description_1 LIKE weather_description_7 THEN 1 ELSE 0 END AS weather_description_7_match
FROM forecast_weather_flat_stream
GROUP BY 1,2,3,4,5,6,7,8;

DROP TABLE IF EXISTS city_score_weather_description;
CREATE TABLE city_score_weather_description AS
SELECT
  sum.city_id AS city_id,
  ROUND((sum_2*1.0/count)::numeric, 2) AS score_2,
  ROUND((sum_3*1.0/count)::numeric, 2) AS score_3,
  ROUND((sum_4*1.0/count)::numeric, 2) AS score_4,
  ROUND((sum_5*1.0/count)::numeric, 2) AS score_5,
  ROUND((sum_6*1.0/count)::numeric, 2) AS score_6,
  ROUND((sum_7*1.0/count)::numeric, 2) AS score_7
FROM
  ( 
  SELECT
    city_id,
  SUM(weather_description_2_match) AS sum_2,
  SUM(weather_description_3_match) AS sum_3,
  SUM(weather_description_4_match) AS sum_4,
  SUM(weather_description_5_match) AS sum_5,
  SUM(weather_description_6_match) AS sum_6,
  SUM(weather_description_7_match) AS sum_7
  FROM city_weather_description_match
  GROUP BY city_id) as sum
JOIN
  (
  SELECT
    city_id,
    count(DISTINCT retrieval_date) as count
  FROM city_weather_description_match
  GROUP BY 1
  ) as count
ON sum.city_id = count.city_id;
  
