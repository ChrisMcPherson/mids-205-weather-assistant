package com.weather;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;


public class Weather {

	public static String WORKING_DIRECTORY = PropertiesUtil.getProperty("WORKING_DIRECTORY");
	public static String CONFIG_DIRECTORY = PropertiesUtil.getProperty("CONFIG_DIRECTORY");
	public static String OPEN_WEATHER_API_KEY = PropertiesUtil.getProperty("OPEN_WEATHER_API_KEY");
	public static String S3_BUCKET_NAME="weather-raw-json";
	
	private final static Logger logger = Logger.getLogger(Weather.class);

	public String processingFolderName;
	public String processingDate;
	public int processingHour;
	
	public void getWeatherData() {
		try {
			//initialize
			initialize();
			
			//fetch current weather
			logger.debug("Fectching Current Weather");			
			String url = "http://api.openweathermap.org/data/2.5/weather?id=";
			Hashtable<Integer, ZoneWeather> currentWeatherLog = new Hashtable<Integer, ZoneWeather>();
			List<Integer> zones = getZonesForCurrentWeatherProcessing(CONFIG_DIRECTORY+"trigger_list_current_weather.csv");
			for(Integer zone : zones) {
				ZoneWeather zoneWeather = new ZoneWeather();
				zoneWeather.getCurrentWeather(zone, processingFolderName);
				currentWeatherLog.put(new Integer(zone), zoneWeather);
			}
			logger.debug("Done Fetching Current Weather");
			
			//fetch forecast
			logger.debug("Fectching Forecast");
			Hashtable<Integer, ZoneWeather> forecastWeatherLog = new Hashtable<Integer, ZoneWeather>();
			ZoneWeather zoneWeather = new ZoneWeather();
			url ="http://api.openweathermap.org/data/2.5/forecast/daily?id=";
			int zoneId = getZonesForForecastProcessing(CONFIG_DIRECTORY+"trigger_list_forecast_weather.csv");
			zoneWeather.getForecastWeather(zoneId, processingFolderName);
			forecastWeatherLog.put(new Integer(zoneId),zoneWeather);
			logger.debug("Done Fetching Forecast");
			
			//upload files to S3
			AWSUtil awsUtil = new AWSUtil();
			awsUtil.transferFilesToS3(WORKING_DIRECTORY, processingDate, ""+processingHour, S3_BUCKET_NAME);

			//write to kafka
			KafkaClient kafkaClient = new KafkaClient();
			kafkaClient.writeToKafkaQueue(WORKING_DIRECTORY + processingDate+"-"+processingHour);

			MailUtil.sendApplicationAlert(currentWeatherLog, forecastWeatherLog, awsUtil, kafkaClient, processingHour);
			
			logger.debug("Process completed");
			System.exit(0);
		} catch(Exception ex) {				
			logger.error(ex, ex);
			MailUtil.sendApplicationAlert("Eror while processing data for "+processingDate+" and "+processingHour, ex);
			System.exit(0);
		}		
	}
		
	private void initialize() throws Exception {
		//format current data
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy");
		Date date = new Date();
		processingDate = sdf.format(date);
		
		//get current hour
		Calendar rightNow = Calendar.getInstance();
		processingHour = rightNow.get(Calendar.HOUR_OF_DAY);
		
		processingFolderName = processingDate +"-"+processingHour;
		
		//Create directory
        File file = new File(WORKING_DIRECTORY+processingFolderName);
        if (!file.exists()) {
            if (file.mkdir()) {
                logger.debug("Directory "+ processingFolderName+ " created!");
            } else {
                logger.error("Failed to create directory - "+ processingFolderName);
                throw new Exception("Failed to create directory!");
            }
        }
        
		//Initialize Root Logger
		WLogger.initRootLogger(WORKING_DIRECTORY+processingFolderName+"/processing-log.txt");		
	}
	
	private List<String> loadCities(int zoneId) throws Exception {
		List<String> cityList = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(CONFIG_DIRECTORY+"citi_list.csv"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] row = line.split(",");
				if(row[0].equals(""+zoneId)) {
					cityList.add(row[1]);
				}
			}
		} catch(Exception ex) {
			logger.error(ex, ex);
			throw ex;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				logger.error(e, e);
			}
		}
		logger.debug("Loaded "+cityList.size()+" cities for zone:"+zoneId);
		return cityList;
	}

	
	private int getZonesForForecastProcessing(String file) throws Exception {
		logger.debug("Processing hour: "+processingHour);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] row = line.split(",");
				if(row[0].equals(processingHour+"")) {
					return Integer.parseInt(row[1]); 
				}
			}
			throw new Exception (file +"  not configured properly for hour"+processingHour);
		} catch(Exception ex) {
			logger.error(ex, ex);
			throw ex;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				logger.error(e, e);
			}
		}
	}
	
	private List<Integer> getZonesForCurrentWeatherProcessing(String file) throws Exception {
		logger.debug("Processing hour: "+processingHour+file);
		List<Integer> list = new ArrayList<Integer>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] row = line.split(",");
				if(row[0].equals(processingHour+"")) {
					list.add(new Integer(row[1]));
					list.add(new Integer(row[2]));
					list.add(new Integer(row[3]));
					list.add(new Integer(row[4]));
					list.add(new Integer(row[5]));
					list.add(new Integer(row[6]));
					list.add(new Integer(row[7]));
					list.add(new Integer(row[8]));
				}
			}
			logger.debug("Zones for processing current weather: "+list.toString());
			return list;
		} catch(Exception ex) {
			logger.error(ex, ex);
			throw ex;
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				logger.error(e, e);
			}
		}
	}
	
	public static void main(String[] args) {
		try {
			Weather weather = new Weather();
			weather.getWeatherData();
		} catch (Exception e) {
			logger.error(e, e);
		}
	}

}
