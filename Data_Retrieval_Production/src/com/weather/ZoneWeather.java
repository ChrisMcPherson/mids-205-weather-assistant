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
import org.json.JSONObject;


public class ZoneWeather {
	private final static Logger logger = Logger.getLogger(ZoneWeather.class);

	private List<String> errorLog = new ArrayList();		
	private int processCount = 0;
	
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
		return errorLog.size();
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

	private void fetchData(List<String> cities, String partialUrl, String tagName) throws Exception {
		BufferedWriter bw = null;
		String fileName = "";
		String url = "";
		try {
			for(String city: cities) {
				logger.debug("Pulling data for City Id: "+city);
				url = partialUrl + city+ "&APPID="+Weather.OPEN_WEATHER_API_KEY;
				String response = callAPI(url, city, tagName, 1);
				
				//try second time
				if(isErrorResponse(response)) {
					//Thread.currentThread().sleep(20);
					response = callAPI(url, city, tagName, 2);

					if(isErrorResponse(response)) {
						logger.debug("Failed to process city "+city+", URL-"+url);
						logger.debug("Response "+response);
						String error = "Failed to process city Id: "+ city +"; Type: "+ tagName + "; Request URL: "+url+"; Response: "+response;
						errorLog.add(error);
						continue;
					}
				}
				SimpleDateFormat sdf = new SimpleDateFormat("HH-mm-ss");
				Date date = new Date();
				fileName = Weather.WORKING_DIRECTORY+processingFolderName+"/"+city+"-"+tagName +"-" + sdf.format(date)+".json";
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

	private String callAPI(String url, String city, String tagName, int retryCount) throws Exception {
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
			return responseData;
		} catch(Exception ex) {
			logger.error(ex, ex);
			return responseData;
		} finally {
			try {
				if (client != null) {
					client.close();
				}
			} catch (Exception tx) {
				// do nothing
			}
		}
	}
	
	public static boolean isErrorResponse(String responseData) {
		if(responseData == null || responseData.equals("")) {
			return true;
		}
		if(responseData.contains("Bad Gateway")) {
			return true;
		}
		try {
			JSONObject jsonObject = new JSONObject(responseData.toString());
			String errorCode = jsonObject.get("cod").toString().replace("\"", "");
			if(Integer.parseInt(errorCode.trim()) == 200) {
				return false;
			}
		} catch(Exception e) {
			logger.error(e, e);
			//invalid JSON means error
		}
		return true;
	}
}
