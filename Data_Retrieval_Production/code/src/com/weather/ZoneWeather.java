package com.weather;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ZoneWeather {
	private final static Logger logger = Logger.getLogger(ZoneWeather.class);

	private List<String> errorLog = new ArrayList();		
	private int processCount = 0;
	private int errorCount = 0;
	
	private String processingFolderName;

	public void getCurrentWeather(int zone, String folderName) throws Exception {
		logger.debug("Processing current weather for zone "+zone);
		this.processingFolderName=folderName;
		
		List<String> cities = loadCities(zone);
		processCount = cities.size();			
		String url = "http://api.openweathermap.org/data/2.5/weather?id=";			
		fetchData(cities, url, "currrent-weather");
	}

	public void getForecastWeather(int zone, String folderName) throws Exception {
		logger.debug("Processing forecast weather for zone "+zone);
		this.processingFolderName=folderName;
		
		List<String> cities = loadCities(zone);
		processCount = cities.size();	
		String url ="http://api.openweathermap.org/data/2.5/forecast/daily?id=";
		fetchData(cities, url, "forecast-weather");
	}
	
	public List<String> getErrorLog() {
		return errorLog;
	}
	
	public int getProcessCount() {
		return processCount;
	}
	
	public int getErrorCount() {
		return errorCount;
	}

	private List<String> loadCities(int zoneId) throws Exception {
		List<String> cityList = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(Weather.CONFIG_DIRECTORY+"citi_list.csv"));
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

	private void fetchData(List<String> cities, String partialUrl, String partialName) throws Exception {
		BufferedWriter bw = null;
		String fileName = "";
		String url = "";
		try {
			for(String city: cities) {
				logger.debug("Pulling data for City Id: "+city);
				url = partialUrl + city+ "&APPID="+Weather.OPEN_WEATHER_API_KEY;
				String response = callAPI(url, city, partialName);
				//if 502 Bad Gateway
				if(response.contains("502 Bad Gateway")) {
					Thread.currentThread().wait(100);
					response = callAPI(url, city, partialName);
				}
				SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss");
				Date date = new Date();
				fileName = Weather.WORKING_DIRECTORY+processingFolderName+"/"+city+"-"+partialName +"-" + sdf.format(date)+".json";
				File file = new File(fileName);
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter fw = new FileWriter(file.getAbsoluteFile());
				bw = new BufferedWriter(fw);
				bw.write(response);
				bw.close();
				if(true) {
					break;
				}
			}
		} catch(Exception ex) {
			logger.debug("Unable to create - "+fileName);
			logger.error(ex, ex);
			throw ex;
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				logger.error(e, e);
			}
		}
	}

	private String callAPI(String url, String city, String tagName) throws Exception {
		String responseData = "";
		CloseableHttpClient client = HttpClients.createDefault();
		try {
			HttpGet httpGet = new HttpGet(url);
			CloseableHttpResponse response = client.execute(httpGet);
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String inputLine = "";
			while((inputLine = rd.readLine()) != null)	{
				responseData += inputLine;
			}
			JsonParser jsonParser = new JsonParser();		    
			JsonObject jsonObject = (JsonObject)jsonParser.parse(responseData.toString());
			String errorCode = jsonObject.get("cod").toString().replace("\"", "");
			if(Integer.parseInt(errorCode.trim()) != 200) {
				String error = "Failed to process city Id: "+ city +"; Type: "+ tagName + "; Request URL: "+url+"; Response: "+responseData;
				errorLog.add(error);
			}
		} catch(Exception ex) {
			logger.debug("Failed to process city "+city+", URL-"+url);
			logger.debug("Response "+responseData);
			String error = "Failed to process city Id: "+ city +"; Type: "+ tagName + "; Request URL: "+url+"; Response: "+responseData;
			errorLog.add(error);
		} finally {
			try {
				if (client != null) {
					client.close();
				}
			} catch (Exception tx) {
				// do nothing
			}
		}
		return responseData;
	}
}
