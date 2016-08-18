SELECT
  d.city_id AS city_id,
  c.name AS city_name,
  c.country_code AS country_code,
  (score_2 + score_3 + score_4 + score_5 + score_6 + score_7)/ 6.0 AS avg_score
FROM
  city_score_weather_description AS d
JOIN cities AS c
  on d.city_id = c.id
GROUP BY city_id, name, country_code, score_2, score_3, score_4, score_5, score_6, score_7
ORDER BY avg_score
LIMIT 10;
