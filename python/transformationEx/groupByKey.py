from pyspark import SparkConf, SparkContext
classScore = sc.parallelize({('class1', 70),('class2', 75),('class2', 40),('class3', 10),('class1', 90),('class2', 100)})
classScoreGroup = classScore.groupByKey().map(lambda classObject:(classObject[0], list(classObject[1])))
classScoreGroup.collect()
