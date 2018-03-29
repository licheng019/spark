from pyspark import SparkConf, SparkContext
from operator import add
classScore = sc.parallelize({('class1', 70),('class2', 75),('class2', 40),('class3', 10),('class1', 90),('class2', 100)})
classScoreGroup = classScore.sortByKey()
classScoreGroup.collect()
