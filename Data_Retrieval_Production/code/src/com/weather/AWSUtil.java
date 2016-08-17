package com.weather;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;

public class AWSUtil {
	
	private final static Logger logger = Logger.getLogger(AWSUtil.class);
	
	private String errorMessage = "";
	private boolean status = true;
	
	public void transferFilesToS3(String localProjectDirector, String currentDate, String currentHour, String bucketName) throws Exception {
		System.setProperty("aws.accessKeyId", PropertiesUtil.getProperty("AWS_ACCESS_KEY_ID"));
		System.setProperty("aws.secretKey", PropertiesUtil.getProperty("AWS_SECRET_ACCESS_KEY"));
		AmazonS3 s3Client = new AmazonS3Client();
		try {
			logger.debug("Started uploading files to S3");			
			File file = new File(localProjectDirector + currentDate +"-"+currentHour + "/");
		    TransferManager tm = new TransferManager(s3Client);
		    MultipleFileUpload upload = tm.uploadDirectory(bucketName, currentDate +"/"+currentHour, file, true);
		} catch (Exception ex) {
			logger.error(ex, ex);
			errorMessage = ex.getMessage();
			status = false;
		}
	}

	public boolean getStatus() {
		return status;
	}
	
	public String getErrorMessage() {
		return errorMessage;
	}
	
	public static void main(String[] args) {
		try {
			AWSUtil util = new AWSUtil();			
			util.transferFilesToS3("C:/Users/rthamman/Dropbox (Personal)/Berkeley/Courses/W205/Project/","06-22-2016", "23" ,"weather-raw-json");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
