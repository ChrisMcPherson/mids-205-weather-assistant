from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreamingKafkaWordCount")
#interval of 1 second
ssc = StreamingContext(sc, 2)

kvs = KafkaUtils.createDirectStream(ssc, ["test_stream"], {"metadata.broker.list": "ec2-54-152-39-8.compute-1.amazonaws.com:9092"})
lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()
ssc.awaitTermination()