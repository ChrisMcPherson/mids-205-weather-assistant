# W205 Final Project WFE Analysis

## elevation_by_country.csv
- The data was taken from Wikipedia page (https://en.wikipedia.org/wiki/List_of_countries_by_average_elevation) and 
- cleaned by running the OpenRefine workbook (elevation_by_country.openrefine.tar.gz).
- After cleaning we can upload the final product (elevation_by_country.csv) to Postgres by running upload_elevation_by_country.sql

##country-code-and-name.csv
country code and country name mapping

##w205_fp_viz.twb
- Tableau file that created this live dashboard
- Live Dashboard can be viewed here https://public.tableau.com/shared/SCPY6MPY5?:display_count=yes

##temperature_sd_by_city.sql
Calculate temperature deviation by city and create table city_sd_temperature, which was used for Tableau visualization

##humidity_sd_by_city.sql
Calculate humidity deviation by city and create table city_sd_humidity, which was used for Tableau visualization

##city_score_weather_description.sql
Calculate weather description forecast accuracy score by city and create table city_score_weather_description, which was used for Tableau visualization

## top_10_humidity.sql
Find top 10 cities that had the best humidity forecast predictions

## worst_10_weather_description_forecasts.sql
Find top 10 cities that had the worst weather description forecasts

##rio_temp_deviation_during_olympic.sql
Find Rio temp deviation in forecast from actual temperature during the Olympics
