package w205Final.weatherEvaluator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class Producer {
	
	public static void main(String[] args) throws IOException {
		
		KafkaProducer<String, String> producer;
		try (InputStream props = Resources.getResource("producer.properties").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		}
		try {
			producer.send(new ProducerRecord<String, String>("weather_data", "JSON Payload Goes Here"));
			producer.flush();
		} finally {
			producer.close();
		}
		
	}
}