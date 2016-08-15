package w205Final.weatherEvaluator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Producer {
	
	public static boolean isErrorResponse(String responseData) {
		if (responseData == null || responseData.equals("")) {
			return true;
		}
		if (responseData.contains("Bad Gateway")) {
			return true;
		}
		try {
			JSONObject jsonObject = new JSONObject(responseData.toString());
			String errorCode = jsonObject.get("cod").toString().replace("\"", "");
			if (Integer.parseInt(errorCode.trim()) == 200) {
				return false;
			}
		} catch (Exception e) {
			System.out.println(e);
			return true;
		}
		return true;
	}
	
	public static void main(String[] args) throws IOException {
		// Configure kafka producer from properties file
		KafkaProducer<String, String> producer;
		
		String filePath = "producer.properties";
		try (InputStream in = Producer.class.getClassLoader().getResourceAsStream(filePath)) {
			Properties properties = new Properties();
			properties.load(in);
			
			producer = new KafkaProducer<>(properties);
		}
		
		try {
			// Connect to s3 to retrieve files to load to kafka
			String bucketName = "weather-raw-json";
			System.setProperty("aws.accessKeyId", "AKIAJEELMKR3NAPXHURA");
			System.setProperty("aws.secretKey", "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");
			AmazonS3Client s3Client = new AmazonS3Client();
			
			// final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
			final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
			ListObjectsV2Result result;
			int fileProcessCount = 0;
			do {
				result = s3Client.listObjectsV2(req);
				for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					String key = objectSummary.getKey();
					String line;
					String json = "";
					S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));
					BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
					while ((line = reader.readLine()) != null) {
						json += line;
					}
					if (isErrorResponse(json)) {
						System.out.println("Error " + json);
						continue;
					}
					Integer forecast = 0;
					if (key.contains("forecast-weather")) {
						forecast = 1;
					}
					StringTokenizer tokenizer = new StringTokenizer(key, "/");
					String retrievalDate = tokenizer.nextToken();
					String hour = tokenizer.nextToken();
					
					// Add attributes from filename as new elements in json
					JSONObject jsonObject = new JSONObject(json.toString());
					jsonObject.put("forecast", forecast);
					jsonObject.put("retrievalDate", retrievalDate);
					jsonObject.put("hour", hour);
					
					fileProcessCount++;
					System.out.println(fileProcessCount + "\n" + key + "\n" + jsonObject.toString() + "\n" + json);
					
					// drop json onto kafka topic
					producer.send(new ProducerRecord<String, String>("test_stream", jsonObject.toString()));
					producer.flush();
				}
				req.setContinuationToken(result.getNextContinuationToken());
			} while (result.isTruncated() == true);
			System.out.println("File Process Count :" + fileProcessCount);
		} catch (Exception ex) {
			System.out.println("Error during s3 object identification");
			ex.printStackTrace();
		} finally {
			producer.close();
		}
		
		
	}
}
