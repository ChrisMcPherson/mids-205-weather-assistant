
package org.apache.storm.starter.bolt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.storm.starter.tools.DailyForecast;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;


public class WeatherStreamBolt extends BaseBasicBolt {
	
	private Connection c = null;
	
	@Override
	public void prepare(Map conf, TopologyContext context) {
		try {
			Class.forName("com.facebook.presto.jdbc.PrestoDriver");
			c = DriverManager.getConnection("jdbc:presto://ec2-54-152-39-8.compute-1.amazonaws.com:8889/hive", "hadoop", "");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println(tuple);
		JSONObject json = new JSONObject(tuple.toString());
		DailyForecast myForecast = new DailyForecast(json);
		String sql = myForecast.getFlattenedSQLQuery("", "", ""); // =========== remember to parse json for initial elements to insert into correct db
		
		Statement st = null;
		try {
			st = c.createStatement();
			st.executeUpdate(sql);
		} catch (Exception ex) {
			System.out.println("Sql " + sql);
			ex.printStackTrace();
		} finally {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void cleanup() {
		try {
			c.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {}
	
}
