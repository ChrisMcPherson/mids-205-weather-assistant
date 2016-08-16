SELECT
  d1.city_id AS city_id,
  d1.city_name AS city_name,
  d1.country_code AS country_code,
  round(CAST(|/(SUM(d1.delta_2_1)/ d2.date_count) AS NUMERIC), 2) AS sd_2,
  round(CAST(|/(SUM(d1.delta_3_1)/ d2.date_count) AS NUMERIC), 2) AS sd_3,
  round(CAST(|/(SUM(d1.delta_4_1)/ d2.date_count) AS NUMERIC), 2) AS sd_4,
  round(CAST(|/(SUM(d1.delta_5_1)/ d2.date_count) AS NUMERIC), 2) AS sd_5,
  round(CAST(|/(SUM(d1.delta_6_1)/ d2.date_count) AS NUMERIC), 2) AS sd_6,
  round(CAST(|/(SUM(d1.delta_7_1)/ d2.date_count) AS NUMERIC), 2) AS sd_7
FROM
  (
  SELECT
    city_id,
    city_name,
    country_code,
    retrieval_date,
    (day_temp_2 - day_temp_1)^2 AS delta_2_1,
    (day_temp_3 - day_temp_1)^2 AS delta_3_1,
    (day_temp_4 - day_temp_1)^2 AS delta_4_1,
    (day_temp_5 - day_temp_1)^2 AS delta_5_1,
    (day_temp_6 - day_temp_1)^2 AS delta_6_1,
    (day_temp_7 - day_temp_1)^2 AS delta_7_1
  FROM forecast_weather_flat
  WHERE
    country_code = 'BR'
    AND city_name = 'Rio de Janeiro'
    AND retrieval_date BETWEEN '08-03-2016' AND '08-21-2016'
  GROUP BY city_id, city_name, country_code, retrieval_date, day_temp_1, day_temp_2, day_temp_3, day_temp_4, day_temp_5, day_temp_6, day_temp_7
  ) AS d1
JOIN
  (SELECT
    city_id,
    COUNT(DISTINCT retrieval_date) AS date_count
  FROM forecast_weather_flat
  WHERE
    retrieval_date BETWEEN '06-24-2016' AND '06-27-2016'
  GROUP BY 1
  ) AS d2
ON
  d1.city_id = d2.city_id
GROUP BY d1.city_id, d1.city_name, d1.country_code, d2.date_count;
