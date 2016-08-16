package com.weather;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.parser.CurrentWeather;
import com.parser.DailyForecast;

public class DbUtil {

	private final static Logger logger = Logger.getLogger(DbUtil.class);

	public Connection getConnection() throws Exception {
		Class.forName("org.postgresql.Driver");
		Connection c = DriverManager
				.getConnection("{server url}", "{password}");
		c.setAutoCommit(true);
		return c;
	}

	public void writeToDB(String bucketName, String prefix, String cityFilter) {
		Connection c = null;
		Statement st = null;
		ResultSet rs = null;
		String sql = "";
		try {
			c = getConnection();
			System.setProperty("aws.accessKeyId","{aws key id}");
			System.setProperty("aws.secretKey","{aws secret key}");
			AmazonS3Client s3Client = new AmazonS3Client();

			//final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
			final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
			ListObjectsV2Result result;
			int fileProcessCount = 0;
			do {               
				result = s3Client.listObjectsV2(req);
				for (S3ObjectSummary objectSummary : 
					result.getObjectSummaries()) {
					String key = objectSummary.getKey(); 
					
					String cityCode = getCityCode(key);
					if(!cityFilter.contains(cityCode)) {
						continue;
					}					

					String line;
					String json = "";
					S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, key));   	   			
					BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
					while((line = reader.readLine()) != null) {
						json += line;
					}
					if(isErrorResponse(json)) {
						System.out.println("Error "+json);
						continue;
					}				
					
					StringTokenizer tokenizer = new StringTokenizer(key, "/");
					String date = tokenizer.nextToken();
					String hour = tokenizer.nextToken();

					if(key.contains("currrent-weather")) {
						//CurrentWeather data = new CurrentWeather(new JSONObject(json));
						//sql = data.getSQLQuery(date, hour, key);
						continue;
					} else {
						DailyForecast data = new DailyForecast(new JSONObject(json));
						sql = data.getFlattenedSQLQuery(date, hour, key);
					}
					//System.out.println(json);
					fileProcessCount ++;
					//System.out.println(sql);
					System.out.println(fileProcessCount);
					st = c.createStatement();
					st.executeUpdate(sql);
				}
				req.setContinuationToken(result.getNextContinuationToken());
			} while(result.isTruncated() == true ); 
			System.out.println("File Process Count :"+fileProcessCount);
		} catch(Exception ex) {
			System.out.println("Sql "+sql);
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

			} catch(Exception ce) {
				//nothing
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
			System.out.println(e);
			return true;
		}
		return true;
	}
	
	public void getFolderCount(String bucketName, String prefix, String fileNameFilter) {
		try {
			System.setProperty("aws.accessKeyId","{Key Id}");
			System.setProperty("aws.secretKey","{Secret Key}");
			AmazonS3Client s3Client = new AmazonS3Client();

			final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(prefix);
			ListObjectsV2Result result;
			int count = 0;
			do {               
				result = s3Client.listObjectsV2(req);
				for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
					String key = objectSummary.getKey();
					if(key.contains(fileNameFilter)) {
						count++;
					}					
					req.setContinuationToken(result.getNextContinuationToken());
				}
			} while(result.isTruncated() == true );
			System.out.println("Total Count "+count);
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public String getCityCode(String keyName) {
		int first = keyName.indexOf("/");
		int second = keyName.indexOf("/", first + 1);
		first = keyName.indexOf("-", second + 1);
		String cityCode = keyName.substring(second + 1, first);
		return cityCode;
	}

	public static void main(String args[]) {
		DbUtil dbUtil = new DbUtil();
		String cityFilter = "4031742,5866583,5354943,5364782,5030856,4883207,3616253,3652567,5126246,3726540,2511287,2974942,2609906,711635,362277,742987,481608,289915,2073140,1683852,1847968,2169237,2094027,2181742";
		dbUtil.writeToDB("weather-raw-json", "07-04-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-05-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-06-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-07-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-08-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-09-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-10-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-11-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-12-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-13-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-14-2016", cityFilter);
		dbUtil.writeToDB("weather-raw-json", "07-15-2016", cityFilter);

	}



}
