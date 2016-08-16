package com.weather;

import java.io.File;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class KafkaClient {

	final static Logger logger = Logger.getLogger(KafkaClient.class);
	private String errorMessage = "";
	private boolean status = true;
	
	public void writeToKafkaQueue(String folderName) {
		KafkaProducer<String, String> producer = null;
		try {
			producer = new KafkaProducer<String, String>(PropertiesUtil.getApplicationProperties());
			File[] files = new File(folderName).listFiles();
			for (File file : files) {				
			    if (file.isFile() && file.getName().contains(".json")) {
			    	System.out.println("Transferring file name "+file.getName());
					producer.send(new ProducerRecord<String, String>("weather_data", Files.toString(file, Charsets.UTF_8)));
					producer.flush();
			    }
			}
			logger.debug("Wrote "+files.length+ " files to Kafka queue");
		} catch(Exception ex) {
			logger.error(ex, ex);
			errorMessage = ex.getMessage();
			status = false;
		}
		finally {
			if(producer != null) {
				producer.close();
			}
		}
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	
	public boolean getStatus() {
		return status;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaClient client = new KafkaClient();
		try {
			client.writeToKafkaQueue("C:/Users/rthamman/Dropbox (Personal)/Berkeley/Courses/W205/Project/06-23-2016-17");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
