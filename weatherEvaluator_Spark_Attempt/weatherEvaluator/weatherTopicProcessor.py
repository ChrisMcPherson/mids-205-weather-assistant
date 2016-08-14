# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext

#spark context with 2 working threads
sc = SparkContext("local[2]","test app")

sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
people = sqlContext.read.json("/data/spark15/examples/18918-currrent-weather-01-05-40.json")

# Displays the content of the DataFrame to stdout
#df.show()

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: integer (nullable = true)
#  |-- name: string (nullable = true)

# Register this DataFrame as a table.
#people.registerTempTable("people")

# SQL statements can be run by using the sql methods provided by `sqlContext`.
#teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Alternatively, a DataFrame can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
#anotherPeopleRDD = sc.parallelize([
#  '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
#anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)