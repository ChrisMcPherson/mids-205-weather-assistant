This code alows us to run a batch import of the current weather data archived in S3. It outputs the deserialized and enhanced json data into our external Hive tables through Presto (insert statements through Presto are about 20 times faster than Hive inserts to the same Hive table).

The Producer.java file holds the main() method. 