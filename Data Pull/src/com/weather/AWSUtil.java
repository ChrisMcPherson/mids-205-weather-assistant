package com.weather;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;

public class AWSUtil {
	
	private final static Logger logger = Logger.getLogger(AWSUtil.class);
	
	private String errorMessage = "";
	private boolean status = true;	
	AmazonS3 s3Client = null;
	
	public void AWSUtil() {
		System.setProperty("aws.accessKeyId", PropertiesUtil.getProperty("AWS_ACCESS_KEY_ID"));
		System.setProperty("aws.secretKey", PropertiesUtil.getProperty("AWS_SECRET_ACCESS_KEY"));
		s3Client = new AmazonS3Client();
	}
	
	public void transferFilesToS3(String localProjectDirector, String currentDate, String currentHour, String bucketName) throws Exception {

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
	
	public String readFromS3(String bucketName, String key) throws IOException {
		System.setProperty("aws.accessKeyId","AKIAJEELMKR3NAPXHURA");
		System.setProperty("aws.secretKey","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");
		s3Client = new AmazonS3Client();

        final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;
        do {               
           result = s3Client.listObjectsV2(req);
           
           for (S3ObjectSummary objectSummary : 
               result.getObjectSummaries()) {
               System.out.println(" - " + objectSummary.getKey() + "  " +
                       "(size = " + objectSummary.getSize() + 
                       ")");
           }
           System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
           req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated() == true ); 
		
		
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));
		System.out.println(s3object.getObjectMetadata().getContentType());
		System.out.println(s3object.getObjectMetadata().getContentLength());
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		String json = "";
		while((line = reader.readLine()) != null) {
			json += line;
		}
		return json;
	}
	
	
	public static void main(String[] args) {
		try {
			AWSUtil util = new AWSUtil();
			System.out.println(util.readFromS3("weather-raw-json", "06-24-2016/0/1005029-forecast-weather-00-02-38.json"));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
