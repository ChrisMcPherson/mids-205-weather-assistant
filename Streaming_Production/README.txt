The code in this folder is composed of two related by independent parts. The weatherEvaluatorInitiator folder holds the code to drop all weather data (in json format) from the S3 archive onto a Kafka topic. The storm-starter folder holds the code for the Storm topology that consumes the data from the Kafka topic, deserializes and enhances the json, and outputs the data to external Hive tables for consumption by the Presto query engine.


Instructions:
Execute the pipeline initialization .jar. This executable drops all weather data (in json format) from S3 archive onto a Kafka topic. The Storm topology is already running and waiting to process incoming files. This execution performs a replay of all captured data. A pattern that is essential to managing a streaming pipeline built on the Kappa architecture fundamentals. 

$ cd ~/mids-205-weather-assistant/
$ java -jar initiate-stream.jar
