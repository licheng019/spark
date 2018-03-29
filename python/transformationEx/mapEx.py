from pyspark import SparkConf, SparkContext
numbers = sc.parallelize({1,2,3,4,5})
newNumbers = numbers.map(lambda number: number * 2)
newNumbers.collect()
