This code alows us to run a batch import of the current weather data archived in S3. It outputs the deserialized and enhanced json data into our external Hive tables through Presto (insert statements through Presto are about 20 times faster than Hive inserts to the same Hive table). 

The Hive tables that are populated are:
w205_final.current_weather
w205_final.forecast_weather
w205_final.forecase_weather_flat

The .jar for the compiled maven project is included. Use the below instructions to run:
$ cd ~/mids-205-weather-assistant/Batch_Production/
$ java -jar initiate-batch.jar

*The Producer.java file holds the main() method. 

