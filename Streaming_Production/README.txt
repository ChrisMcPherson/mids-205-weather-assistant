The code in this folder is composed of two related but independent parts. The weatherEvaluatorInitiator folder holds the code to drop all weather data (in json format) from the S3 archive onto a Kafka topic. This is used to to initiate the stream as the Kafka queue will be empty. However, in a production system this application would be run to facilitate a replay of the historical data. In a Kappa architecture based system this is how data is replayed, as opposed to a batch layer. The storm-starter folder holds the code for the Storm topology that consumes the data from the Kafka topic, deserializes and enhances the json, and outputs the data to external Hive tables for consumption by the Presto query engine. We expereienced difficulties with the 4 machine storm cluster and implemented a batch version (the Batch_Production folder of this repo) to ensure we had a working version. The code is kept here, however, to show our efforts and to serve as an archive for continued work on the application (yes, Chris McPherson is planning on building this out for personal use!)


Instructions:
Execute the pipeline initialization .jar. This executable drops all weather data (in json format) from S3 archive onto a Kafka topic. The Storm topology is already running and waiting to process incoming files. This execution performs a replay of all captured data. A pattern that is essential to managing a streaming pipeline built on the Kappa architecture fundamentals. 

$ cd ~/mids-205-weather-assistant/Streaming_Production/
$ java -jar initiate-stream.jar
