package com.weather;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


public class PropertiesUtil {
	final static Logger logger = Logger.getLogger(PropertiesUtil.class);

	public static Properties properties = null;
	
	public static Properties getApplicationProperties () {
		Properties prop = new Properties();
		try {
			InputStream input = null;
			//String contentBuilderPath = System.getProperty("user.dir")+"/resources/weather.properties";
			String contentBuilderPath = "/weather/code/resources/weather.properties";
			input = new FileInputStream(contentBuilderPath);
	    	prop.load(input);	    		
		} catch (Exception ex) {
			logger.error(ex,ex);
			//throw ex;
		}
		return prop;
	}
	
	public static String getProperty(String key) {
		if(properties == null) {
			properties = getApplicationProperties();
		}
		return properties.getProperty(key);
	}
		
	
}
