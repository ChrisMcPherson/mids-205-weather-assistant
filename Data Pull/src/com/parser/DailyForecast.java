/*
 * Copyright (c) 2013-2015 Ashutosh Kumar Singh <me@aksingh.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.parser;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONObject;

import com.parser.AbstractWeather.Weather;
import com.parser.DailyForecast.Forecast.Temperature;

/**
 * <p>
 * Parses daily forecast data and provides methods to get/access the same information.
 * This class provides <code>has</code> and <code>get</code> methods to access the information.
 * </p>
 * <p>
 * <code>has</code> methods can be used to check if the data exists, i.e., if the data was available
 * (successfully downloaded) and was parsed correctly.
 * <code>get</code> methods can be used to access the data, if the data exists, otherwise <code>get</code>
 * methods will give value as per following basis:
 * Boolean: <code>false</code>
 * Integral: Minimum value (MIN_VALUE)
 * Floating point: Not a number (NaN)
 * Others: <code>null</code>
 * </p>
 *
 * @author Ashutosh Kumar Singh
 * @version 2014/12/27
 * @see <a href="http://openweathermap.org/forecast">OWM's Weather Forecast API</a>
 * @since 2.5.0.3
 */
public class DailyForecast extends AbstractForecast {
    /*
    Instance variables
     */
    private final List<Forecast> forecastList;

    /*
    Constructors
     */
    public DailyForecast(JSONObject jsonObj) {
        super(jsonObj);

        JSONArray dataArray = (jsonObj != null) ? jsonObj.optJSONArray(JSON_FORECAST_LIST) : new JSONArray();
        this.forecastList = (dataArray != null) ? new ArrayList<Forecast>(dataArray.length()) : Collections.EMPTY_LIST;
        if (dataArray != null && this.forecastList != Collections.EMPTY_LIST) {
            for (int i = 0; i < dataArray.length(); i++) {
                JSONObject forecastObj = dataArray.optJSONObject(i);
                if (forecastObj != null) {
                    this.forecastList.add(new Forecast(forecastObj));
                }
            }
        }
    }

    /**
     * @param index Index of Forecast instance in the list.
     * @return Forecast instance if available, otherwise <code>null</code>.
     */
    public Forecast getForecastInstance(int index) {
        return this.forecastList.get(index);
    }

    /**
     * <p>
     * Parses forecast data (one element in the forecastList) and provides methods to get/access the same information.
     * This class provides <code>has</code> and <code>get</code> methods to access the information.
     * </p>
     * <p>
     * <code>has</code> methods can be used to check if the data exists, i.e., if the data was available
     * (successfully downloaded) and was parsed correctly.
     * <code>get</code> methods can be used to access the data, if the data exists, otherwise <code>get</code>
     * methods will give value as per following basis:
     * Boolean: <code>false</code>
     * Integral: Minimum value (MIN_VALUE)
     * Floating point: Not a number (NaN)
     * Others: <code>null</code>
     * </p>
     */
    public static class Forecast extends AbstractForecast.Forecast {
        /*
        JSON Keys
         */
        public static final String JSON_TEMP = "temp";

        private static final String JSON_FORECAST_PRESSURE = "pressure";
        private static final String JSON_FORECAST_HUMIDITY = "humidity";
        private static final String JSON_FORECAST_WIND_SPEED = "speed";
        private static final String JSON_FORECAST_WIND_DEGREE = "deg";
        private static final String JSON_FORECAST_CLOUDS = "clouds";
        private static final String JSON_FORECAST_RAIN = "rain";
        private static final String JSON_FORECAST_SNOW = "snow";

        /*
        Instance Variables
         */
        private final float pressure;
        private final float humidity;
        private final float windSpeed;
        private final float windDegree;
        private final float cloudsPercent;
        private final float rain;
        private final float snow;

        private final Temperature temp;

        /*
        Constructors
         */
        Forecast() {
            super();

            this.pressure = Float.NaN;
            this.humidity = Float.NaN;
            this.windSpeed = Float.NaN;
            this.windDegree = Float.NaN;
            this.cloudsPercent = Float.NaN;
            this.rain = Float.NaN;
            this.snow = Float.NaN;

            this.temp = new Temperature();
        }

        Forecast(JSONObject jsonObj) {
            super(jsonObj);

            JSONObject jsonObjTemp = (jsonObj != null) ? jsonObj.optJSONObject(JSON_TEMP) : null;
            this.temp = (jsonObjTemp != null) ? new Temperature(jsonObjTemp) : new Temperature();

            this.humidity = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_HUMIDITY, Double.NaN) : Float.NaN;
            this.pressure = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_PRESSURE, Double.NaN) : Float.NaN;
            this.windSpeed = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_WIND_SPEED, Double.NaN) : Float.NaN;
            this.windDegree = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_WIND_DEGREE, Double.NaN) : Float.NaN;
            this.cloudsPercent = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_CLOUDS, Double.NaN) : Float.NaN;
            this.rain = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_RAIN, Double.NaN) : Float.NaN;
            this.snow = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_FORECAST_SNOW, Double.NaN) : Float.NaN;
        }

        public boolean hasHumidity() {
            return !Float.isNaN(this.humidity);
        }

        public boolean hasPressure() {
            return !Float.isNaN(this.pressure);
        }

        public boolean hasWindSpeed() {
            return !Float.isNaN(this.windSpeed);
        }

        public boolean hasWindDegree() {
            return !Float.isNaN(this.windDegree);
        }

        public boolean hasPercentageOfClouds() {
            return !Float.isNaN(this.cloudsPercent);
        }

        public boolean hasRain() {
            return !Float.isNaN(this.rain);
        }

        public boolean hasSnow() {
            return !Float.isNaN(this.snow);
        }

        public float getHumidity() {
            return this.humidity;
        }

        public float getPressure() {
            return this.pressure;
        }

        public float getWindSpeed() {
            return this.windSpeed;
        }

        public float getWindDegree() {
            return this.windDegree;
        }

        public float getPercentageOfClouds() {
            return this.cloudsPercent;
        }

        public float getRain() {
            return this.rain;
        }

        public float getSnow() {
            return this.snow;
        }

        public Temperature getTemperatureInstance() {
            return this.temp;
        }

        /**
         * <p>
         * Parses temperature data and provides methods to get/access the same information.
         * This class provides <code>has</code> and <code>get</code> methods to access the information.
         * </p>
         * <p>
         * <code>has</code> methods can be used to check if the data exists, i.e., if the data was available
         * (successfully downloaded) and was parsed correctly.
         * <code>get</code> methods can be used to access the data, if the data exists, otherwise <code>get</code>
         * methods will give value as per following basis:
         * Boolean: <code>false</code>
         * Integral: Minimum value (MIN_VALUE)
         * Floating point: Not a number (NaN)
         * Others: <code>null</code>
         * </p>
         */
        public static class Temperature implements Serializable {
            private static final String JSON_TEMP_DAY = "day";
            private static final String JSON_TEMP_MIN = "min";
            private static final String JSON_TEMP_MAX = "max";
            private static final String JSON_TEMP_NIGHT = "night";
            private static final String JSON_TEMP_EVENING = "eve";
            private static final String JSON_TEMP_MORNING = "morn";

            private final float dayTemp;
            private final float minTemp;
            private final float maxTemp;
            private final float nightTemp;
            private final float eveTemp;
            private final float mornTemp;

            Temperature() {
                this.dayTemp = Float.NaN;
                this.minTemp = Float.NaN;
                this.maxTemp = Float.NaN;
                this.nightTemp = Float.NaN;
                this.eveTemp = Float.NaN;
                this.mornTemp = Float.NaN;
            }

            Temperature(JSONObject jsonObj) {
                this.dayTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_DAY, Double.NaN) : Float.NaN;
                this.minTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_MIN, Double.NaN) : Float.NaN;
                this.maxTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_MAX, Double.NaN) : Float.NaN;
                this.nightTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_NIGHT, Double.NaN) : Float.NaN;
                this.eveTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_EVENING, Double.NaN) : Float.NaN;
                this.mornTemp = (jsonObj != null) ? (float) jsonObj.optDouble(JSON_TEMP_MORNING, Double.NaN) : Float.NaN;
            }

            public boolean hasDayTemperature() {
                return !Float.isNaN(this.dayTemp);
            }

            public boolean hasMinimumTemperature() {
                return !Float.isNaN(this.minTemp);
            }

            public boolean hasMaximumTemperature() {
                return !Float.isNaN(this.maxTemp);
            }

            public boolean hasNightTemperature() {
                return !Float.isNaN(this.nightTemp);
            }

            public boolean hasEveningTemperature() {
                return !Float.isNaN(this.eveTemp);
            }

            public boolean hasMorningTemperature() {
                return !Float.isNaN(this.mornTemp);
            }

            public float getDayTemperature() {
                return this.dayTemp;
            }

            public float getMinimumTemperature() {
                return this.minTemp;
            }

            public float getMaximumTemperature() {
                return this.maxTemp;
            }

            public float getNightTemperature() {
                return this.nightTemp;
            }

            public float getEveningTemperature() {
                return this.eveTemp;
            }

            public float getMorningTemperature() {
                return this.mornTemp;
            }
        }
    }

	@Override
	public String toString() {
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("getCityInstance()=" + getCityInstance());
				
		for(Forecast forecast: forecastList) {
			sbuf.append(forecast.toString());
		}
		return sbuf.toString();
	}
	
	public String getSQLQuery(String date, String hour, String file_name) {
		String query = "";
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO FORECAST_WEATHER ");
		sql.append("(city_id, city_name,");
		sql.append("dt, day_temp, temp_min, temp_max, temp_night, temp_eve, temp_morn,");
		sql.append("pressure, humidity, wind_speed, wind_degree, pct_clouds, rain, snow,");
		sql.append("weather_name, weather_description, weather_icon, weather_code,");
		sql.append("retrieval_date, hour, file_name) VALUES ");
		for(Forecast forecast: forecastList) {			
			sql.append("(");
			sql.append(getCityInstance().getCityCode() + ", ");
			sql.append("'"+getCityInstance().getCityName()+"'" + ", ");
			sql.append("'"+forecast.getDateTime()+"'" + ", ");
			
			//Temp
			Temperature temp = forecast.getTemperatureInstance();
			if(temp != null && temp.hasDayTemperature()) {
				sql.append(temp.getDayTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMinimumTemperature()) {
				sql.append(temp.getMinimumTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMaximumTemperature()) {
				sql.append(temp.getMaximumTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasNightTemperature()) {
				sql.append(temp.getNightTemperature() + ", ");
			} else {
				sql.append("null, ");
			}	
			if(temp != null && temp.hasEveningTemperature()) {
				sql.append(temp.getEveningTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMorningTemperature()) {
				sql.append(temp.getMorningTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			
			//Other			
			if(forecast.hasPressure()) {
				sql.append(forecast.getPressure() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasHumidity()) {
				sql.append(forecast.getHumidity() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWindSpeed()) {
				sql.append(forecast.getWindSpeed() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWindDegree()) {
				sql.append(forecast.getWindDegree() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasPercentageOfClouds()) {
				sql.append(forecast.getPercentageOfClouds() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasRain()) {
				sql.append(forecast.getRain() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasSnow()) {
				sql.append(forecast.getSnow() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWeatherInstance()) {				
				Weather wc = forecast.getWeatherInstance(0);
				sql.append("'"+wc.getWeatherName()+"'" + ", "); 
				sql.append("'"+wc.getWeatherDescription()+"'" + ",");
				sql.append("'"+wc.getWeatherIconName()+"'" + ",");
				sql.append("'"+wc.getWeatherCode()+"'" + ",");
			} else {
				sql.append(",,,,");
			}
			sql.append("'"+date+"'" + ", ");
			sql.append("'"+hour+"'" + ", ");
			sql.append("'"+file_name+"'" + "),");
		}
		query = sql.toString();
		if(query.endsWith(",")) {
		  query = query.substring(0,query.length() - 1);
		}
		query = query +";";
		return query;
	}
	
	public String getFlattenedSQLQuery(String date, String hour, String file_name) {
		StringBuffer sql = new StringBuffer();
		sql.append("INSERT INTO FORECAST_WEATHER_FLAT ");
		sql.append("(city_id, city_name, country_code,retrieval_date, hour, file_name,");
		sql.append("dt_1, day_temp_1, temp_min_1, temp_max_1, temp_night_1, temp_eve_1, temp_morn_1,");
		sql.append("pressure_1, humidity_1, wind_speed_1, wind_degree_1, pct_clouds_1, rain_1, snow_1,");
		sql.append("weather_name_1, weather_description_1, weather_icon_1, weather_code_1,");
		sql.append("dt_2, day_temp_2, temp_min_2, temp_max_2, temp_night_2, temp_eve_2, temp_morn_2,");
		sql.append("pressure_2, humidity_2, wind_speed_2, wind_degree_2, pct_clouds_2, rain_2, snow_2,");
		sql.append("weather_name_2, weather_description_2, weather_icon_2, weather_code_2,");
		sql.append("dt_3, day_temp_3, temp_min_3, temp_max_3, temp_night_3, temp_eve_3, temp_morn_3,");
		sql.append("pressure_3, humidity_3, wind_speed_3, wind_degree_3, pct_clouds_3, rain_3, snow_3,");
		sql.append("weather_name_3, weather_description_3, weather_icon_3, weather_code_3,");
		sql.append("dt_4, day_temp_4, temp_min_4, temp_max_4, temp_night_4, temp_eve_4, temp_morn_4,");
		sql.append("pressure_4, humidity_4, wind_speed_4, wind_degree_4, pct_clouds_4, rain_4, snow_4,");
		sql.append("weather_name_4, weather_description_4, weather_icon_4, weather_code_4,");
		sql.append("dt_5, day_temp_5, temp_min_5, temp_max_5, temp_night_5, temp_eve_5, temp_morn_5,");
		sql.append("pressure_5, humidity_5, wind_speed_5, wind_degree_5, pct_clouds_5, rain_5, snow_5,");
		sql.append("weather_name_5, weather_description_5, weather_icon_5, weather_code_5,");
		sql.append("dt_6, day_temp_6, temp_min_6, temp_max_6, temp_night_6, temp_eve_6, temp_morn_6,");
		sql.append("pressure_6, humidity_6, wind_speed_6, wind_degree_6, pct_clouds_6, rain_6, snow_6,");
		sql.append("weather_name_6, weather_description_6, weather_icon_6, weather_code_6,");
		sql.append("dt_7, day_temp_7, temp_min_7, temp_max_7, temp_night_7, temp_eve_7, temp_morn_7,");
		sql.append("pressure_7, humidity_7, wind_speed_7, wind_degree_7, pct_clouds_7, rain_7, snow_7,");
		sql.append("weather_name_7, weather_description_7, weather_icon_7, weather_code_7");
		sql.append(") VALUES (");
		int count = 1;
		for(Forecast forecast: forecastList) {			
			if(count == 1) {
				sql.append(getCityInstance().getCityCode() + ", ");
				sql.append("'"+getCityInstance().getCityName()+"'" + ", ");
				sql.append("'"+getCityInstance().getCountryCode()+"'" + ", ");
				sql.append("'"+date+"'" + ", ");
				sql.append("'"+hour+"'" + ", ");
				sql.append("'"+file_name+"'"+ ", ");
			}
			count = count +1;
			sql.append("'"+forecast.getDateTime()+"'" + ", ");
			
			//Temp
			Temperature temp = forecast.getTemperatureInstance();
			if(temp != null && temp.hasDayTemperature()) {
				sql.append(temp.getDayTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMinimumTemperature()) {
				sql.append(temp.getMinimumTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMaximumTemperature()) {
				sql.append(temp.getMaximumTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasNightTemperature()) {
				sql.append(temp.getNightTemperature() + ", ");
			} else {
				sql.append("null, ");
			}	
			if(temp != null && temp.hasEveningTemperature()) {
				sql.append(temp.getEveningTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			if(temp != null && temp.hasMorningTemperature()) {
				sql.append(temp.getMorningTemperature() + ", ");
			} else {
				sql.append("null, ");
			}
			
			//Other			
			if(forecast.hasPressure()) {
				sql.append(forecast.getPressure() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasHumidity()) {
				sql.append(forecast.getHumidity() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWindSpeed()) {
				sql.append(forecast.getWindSpeed() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWindDegree()) {
				sql.append(forecast.getWindDegree() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasPercentageOfClouds()) {
				sql.append(forecast.getPercentageOfClouds() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasRain()) {
				sql.append(forecast.getRain() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasSnow()) {
				sql.append(forecast.getSnow() + ", ");
			} else {
				sql.append("null, ");
			}
			if(forecast.hasWeatherInstance()) {				
				Weather wc = forecast.getWeatherInstance(0);
				sql.append("'"+wc.getWeatherName()+"'" + ", "); 
				sql.append("'"+wc.getWeatherDescription()+"'" + ",");
				sql.append("'"+wc.getWeatherIconName()+"'" + ",");
				sql.append("'"+wc.getWeatherCode()+"'" + ",");
			} else {
				sql.append(",,,,");
			}
		}
		String query = "";
		query = sql.toString();
		if(query.endsWith(",")) {
		  query = query.substring(0,query.length() - 1);
		}
		query = query +");";
		return query;
	}
	
	
	
    
}
