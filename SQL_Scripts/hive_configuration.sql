--== CREATE CURRENT AND FORECAST TABLES =========================
CREATE EXTERNAL TABLE w205_final.current_weather (
   id                   int,
   city_id              int,
   city_name            string,
   dt                   timestamp,
   station_name         string,
   coord_lon            double,
   coord_lat            double,
   temp                 double,
   temp_min             double,
   temp_max             double,
   humidity             double,
   pressure             double,
   wind_speed           double,
   wind_gust            double,
   wind_degree          double,
   rain1h               double,
   rain3h               double,
   pct_cloud            double,
   sunrise              timestamp,
   sunset               timestamp,
   weather_name         string,
   weather_description  string,
   weather_icon         string,
   weather_code         string,
   retrieval_date       string,
   hour                 string,
   file_name            string
)
STORED AS ORC
LOCATION 's3://mids205-weather-data/current_weather/'
tblproperties ("orc.compress"="SNAPPY");


CREATE EXTERNAL TABLE w205_final.forecast_weather (
   id                   int,
   city_id              int,
   city_name            string,
   dt                   timestamp,
   day_temp             double, 
   temp_min             double, 
   temp_max             double, 
   temp_night           double, 
   temp_eve             double, 
   temp_morn            double,
   pressure             double, 
   humidity             double, 
   wind_speed           double, 
   wind_degree          double, 
   pct_clouds           double, 
   rain_1, snow         double,
   weather_name         double, 
   weather_description  string, 
   weather_icon         string, 
   weather_code         string
)
STORED AS ORC
LOCATION 's3://mids205-weather-data/forecast_weather/'
tblproperties ("orc.compress"="SNAPPY");


CREATE EXTERNAL TABLE w205_final.forecast_weather_flat (
   id                   int,
   city_id              int,
   city_name            string,
   country_code         string,
   retrieval_date       string,
   hour                 string,
   file_name            string,
   dt_1                 timestamp,
   day_temp_1           double, 
   temp_min_1           double, 
   temp_max_1           double, 
   temp_night_1         double, 
   temp_eve_1           double, 
   temp_morn_1          double,
   pressure_1           double, 
   humidity_1           double, 
   wind_speed_1         double, 
   wind_degree_1        double, 
   pct_clouds_1         double, 
   rain_1, snow_1       double,
   weather_name_1       double, 
   weather_description_1 string, 
   weather_icon_1       string, 
   weather_code_1       string,
   dt_2                 timestamp,
   day_temp_2           double, 
   temp_min_2           double, 
   temp_max_2           double, 
   temp_night_2         double, 
   temp_eve_2           double, 
   temp_morn_2          double,
   pressure_2           double, 
   humidity_2           double, 
   wind_speed_2         double, 
   wind_degree_2        double, 
   pct_clouds_2         double, 
   rain_2, snow_2       double,
   weather_name_2       double, 
   weather_description_2 string, 
   weather_icon_2       string, 
   weather_code_2       string,
   dt_3                 timestamp,
   day_temp_3           double, 
   temp_min_3           double, 
   temp_max_3           double, 
   temp_night_3         double, 
   temp_eve_3           double, 
   temp_morn_3          double,
   pressure_3           double, 
   humidity_3           double, 
   wind_speed_3         double, 
   wind_degree_3        double, 
   pct_clouds_3         double, 
   rain_3, snow_3       double,
   weather_name_3       double, 
   weather_description_3 string, 
   weather_icon_3       string, 
   weather_code_3       string,
   dt_4                 timestamp,
   day_temp_4           double, 
   temp_min_4           double, 
   temp_max_4           double, 
   temp_night_4         double, 
   temp_eve_4           double, 
   temp_morn_4          double,
   pressure_4           double, 
   humidity_4           double, 
   wind_speed_4         double, 
   wind_degree_4        double, 
   pct_clouds_4         double, 
   rain_4, snow_4       double,
   weather_name_4       double, 
   weather_description_4 string, 
   weather_icon_4       string, 
   weather_code_4       string,
   dt_5                 timestamp,
   day_temp_5           double, 
   temp_min_5           double, 
   temp_max_5           double, 
   temp_night_5         double, 
   temp_eve_5           double, 
   temp_morn_5          double,
   pressure_5           double, 
   humidity_5           double, 
   wind_speed_5         double, 
   wind_degree_5        double, 
   pct_clouds_5         double, 
   rain_5, snow_5       double,
   weather_name_5       double, 
   weather_description_5 string, 
   weather_icon_5       string, 
   weather_code_5       string,
   dt_6                 timestamp,
   day_temp_6           double, 
   temp_min_6           double, 
   temp_max_6           double, 
   temp_night_6         double, 
   temp_eve_6           double, 
   temp_morn_6          double,
   pressure_6           double, 
   humidity_6           double, 
   wind_speed_6         double, 
   wind_degree_6        double, 
   pct_clouds_6         double, 
   rain_6, snow_6       double,
   weather_name_6       double, 
   weather_description_6 string, 
   weather_icon_6       string, 
   weather_code_6       string,
   dt_7                 timestamp,
   day_temp_7           double, 
   temp_min_7           double, 
   temp_max_7           double, 
   temp_night_7         double, 
   temp_eve_7           double, 
   temp_morn_7          double,
   pressure_7           double, 
   humidity_7           double, 
   wind_speed_7         double, 
   wind_degree_7        double, 
   pct_clouds_7         double, 
   rain_7, snow_7       double,
   weather_name_7       double, 
   weather_description_7 string, 
   weather_icon_7       string, 
   weather_code_7       string
)
STORED AS ORC
LOCATION 's3://mids205-weather-data/forecast_weather_flat/'
tblproperties ("orc.compress"="SNAPPY");


--== Custom JSON SERDE Experimentation =========================
--With custom json serde (https://github.com/rcongiu/Hive-JSON-Serde)
ADD JAR /home/hadoop/json-serde-1.3.7-jar-with-dependencies.jar;

CREATE EXTERNAL TABLE weather_data (
  base string,
  clouds struct<all:int>,
  cod int,
  coord struct<lat:double, lon:double>,
  dt int,
  id int,
  main struct<grnd_level:double, humidity:int, pressure:double, sea_level:double, temp:double, temp_max:double, temp_min:double>,
  name string,
  sys struct<country:string, message:double, sunrise:int, sunset:int>,
  weather array<struct<description:string, icon:string, id:int, main:string>>,
  wind struct<deg:int, speed:double>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://mids205-weather-data/';

CREATE EXTERNAL TABLE w205_final.weather_data_orc (
  dt int,
  base string,
  name string,
  cod int,
  id int,
  lat double,
  long double,
  clouds int,
  grnd_level double,
  humidity int,
  pressure double,
  sea_level double,
  temp double,
  temp_max double,
  temp_min double,
  country string,
  message double,
  sunrise int,
  sunset int,
  weather_main_1 string,
  weather_desc_1 string,
  weather_icon_1 string,
  weather_id_1 int,
  wind_deg int,
  wind_speed double
)
STORED AS ORC
LOCATION 's3://mids205-weather-data/weather_data_orc/'
tblproperties ("orc.compress"="SNAPPY");

INSERT INTO TABLE weather_data_orc
SELECT 
 dt,
 base,
 name, 
 cod,
 id,
 coord['lat'],
 coord['lon'],
 clouds['all'],
 main['grnd_level'],
 main['humidity'],
 main['pressure'],
 main['sea_level'],
 main['temp'],
 main['temp_max'],
 main['temp_min'],
 sys[country],
 sys[message],
 sys[sunrise],
 sys[sunset],
 weather[0]['main'],
 weather[0]['description'],
 weather[0]['icon'],
 weather[0]['id'],
 wind[deg],
 wind[speed]
FROM default.weather_data
SORT BY dt;