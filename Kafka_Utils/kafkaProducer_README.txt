The properties for the Kafka producer are in the producer.properties file. Everything is configured to connect to the current Kafka Topic (what they call individual queue's): "weather_data". Line 23 (producer.send(...)) in the second argument is where you will send each individual json payload.  

If using Maven for dependency management, you will need the below dependency included in your pom.xml file:

<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>kafka-clients</artifactId>
	<version>0.10.0.0</version>
</dependency>