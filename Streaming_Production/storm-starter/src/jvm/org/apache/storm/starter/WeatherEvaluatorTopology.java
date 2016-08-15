package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.WeatherStreamBolt;
import org.apache.storm.starter.spout.WeatherStreamSpout;
import org.apache.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WeatherEvaluatorTopology {
	
	public static void main(String[] args) throws InterruptedException {
		try {
			
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("weather-stream-spout", WeatherStreamSpout.buildKafkaSpout());
			// builder.setSpout("signals-spout", new SignalsSpout());
			
			builder.setBolt("weather-stream-bolt", new WeatherStreamBolt());
			
			// builder.setBolt("word-counter", new WordCounter(), 2).shuffleGrouping("word-normalizer").allGrouping("signals-spout", "signals");
			
			Config conf = new Config();
			conf.put("confFile", args[0]);
			conf.setDebug(true);
			// conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); //let's see how the topology does without throttling the incoming tuples
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("weatherProcessing", conf, builder.createTopology());
			
			Thread.sleep(10000);
			cluster.killTopology("weatherProcessing");
			cluster.shutdown();
			
		} catch (Exception ioe) {
			System.out.println("################ Exception thrown ################");
			ioe.printStackTrace();
			
		}
	}
}