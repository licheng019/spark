from pyspark import SparkConf, SparkContext
from operator import add
lines = sc.textFile("wasb://spark-test@chengtest.blob.core.windows.net/wordCountExample")
counts = lines.map(lambda line:len(line)).reduce(add)
print counts
