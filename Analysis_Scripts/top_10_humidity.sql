SELECT
  city_name,
  country_code,
  (sd_2 + sd_3 + sd_4 + sd_5 + sd_6 + sd_7)/ 6.0 AS avg_sd
FROM
  city_sd_humidity
GROUP BY city_name, country_code, sd_2, sd_3, sd_4, sd_5, sd_6, sd_7
ORDER BY avg_sd
LIMIT 10;
