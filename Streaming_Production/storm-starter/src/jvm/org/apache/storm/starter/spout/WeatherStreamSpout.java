/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class WeatherStreamSpout { // extends BaseRichSpout {
	
	// below from http://vishnuviswanath.com/realtime-storm-kafka2.html
	public static KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts("localhost:2181");
		String topic = "weather_data";
		String zkRoot = "/kafka";
		String groupId = "kafka.consumer.group";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return new KafkaSpout(spoutConfig);
	}
	
	/*
	 * SpoutOutputCollector _collector;
	 * Random _rand;
	 * @Override
	 * public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	 * _collector = collector;
	 * _rand = new Random();
	 * }
	 * @Override
	 * public void nextTuple() {
	 * Utils.sleep(100);
	 * String[] sentences = new String[] { "the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago",
	 * "snow white and the seven dwarfs", "i am at two with nature" };
	 * String sentence = sentences[_rand.nextInt(sentences.length)];
	 * _collector.emit(new Values(sentence));
	 * }
	 * @Override
	 * public void ack(Object id) {}
	 * @Override
	 * public void fail(Object id) {}
	 * @Override
	 * public void declareOutputFields(OutputFieldsDeclarer declarer) {
	 * declarer.declare(new Fields("word"));
	 * }
	 */
	
}
