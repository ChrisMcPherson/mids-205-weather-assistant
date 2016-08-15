package w205Final.weatherEvaluator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringTokenizer;

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
	
	public static Connection getPrestoConnection() throws Exception {
		Class.forName("com.facebook.presto.jdbc.PrestoDriver");
		Connection c = DriverManager.getConnection("jdbc:presto://ec2-54-152-39-8.compute-1.amazonaws.com:8889/hive", "hadoop", "");
		//Connection c = DriverManager.getConnection("jdbc:presto://localhost:8889/hive", "hadoop", "");
		return c;
	}
	
	public static void main(String[] args) throws IOException {
		// Configure kafka producer from properties file
		Connection c = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "";
		try {
			// Connect to s3 to retrieve files to load to Hive
			c = getPrestoConnection();
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
					
					StringTokenizer tokenizer = new StringTokenizer(key, "/");
					String date = tokenizer.nextToken();
					String hour = tokenizer.nextToken();
					
					if (key.contains("currrent-weather")) {
						CurrentWeather data = new CurrentWeather(new JSONObject(json));
						sql = data.getSQLQuery(date, hour, key);
						continue;
					} else {
						DailyForecast data = new DailyForecast(new JSONObject(json));
						sql = data.getFlattenedSQLQuery(date, hour, key);
					}
					// System.out.println(json);
					// System.out.println(sql);
					
					st = c.createStatement();
					st.executeUpdate(sql);
					
					fileProcessCount++;
					System.out.println(fileProcessCount);
					// System.out.println(fileProcessCount + "\n" + key + "\n" + json);
					
				}
				req.setContinuationToken(result.getNextContinuationToken());
			} while (result.isTruncated() == true);
			System.out.println("File Process Count :" + fileProcessCount);
		} catch (Exception ex) {
			System.out.println("Error during s3 object identification");
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (c != null) {
					c.close();
				}
				
			} catch (Exception ce) {
				// nothing
			}
		}
		
		/*
		 * // Drop new message to Kafka
		 * KafkaProducer<String, String> producer;
		 * try (InputStream props = Resources.getResource("producer.properties").openStream()) {
		 * Properties properties = new Properties();
		 * properties.load(props);
		 * producer = new KafkaProducer<>(properties);
		 * }
		 * try {
		 * producer.send(new ProducerRecord<String, String>("test_stream", "remote test"));
		 * producer.flush();
		 * } finally {
		 * producer.close();
		 * }
		 */
		
		
	}
}
