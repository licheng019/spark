from pyspark import SparkConf, SparkContext
from operator import add
lines = sc.textFile("wasb://spark-test@chengtest.blob.core.windows.net/wordCountExample")
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(add)
counts.collect()
