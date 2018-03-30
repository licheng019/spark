package com.chengli.spark.ex.sparkCore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationEx {
	public static void main(String[]	args) {
		
		/*map();
		filter();
		groupByKey();
		reduceByKey();
		sortByKey();
		*/
		join();
	}
	
	public static void map() {
		SparkConf conf = new SparkConf().setAppName("map transformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<Integer> numberList = context.parallelize(Arrays.asList(1,2,3,4,5));
		JavaRDD<Integer> numberMultipleTwoList = numberList.map(new Function<Integer, Integer>(){
			@Override
			public Integer call(Integer number) throws Exception {
				
				return number * 2;
			}
			
		});
		List<Integer> list = numberMultipleTwoList.collect();
		for(Integer number: list) {
			System.out.println(number);
		}
		context.close();
	}
	
	public static void filter() {
		SparkConf conf = new SparkConf().setAppName("filter tranformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<Integer> numbers = context.parallelize(Arrays.asList(1,2,3,4,5,6));
		JavaRDD<Integer> evenNumbers = numbers.filter(new Function<Integer, Boolean>(){
			@Override
			public Boolean call(Integer number) throws Exception {
				// TODO Auto-generated method stub
				return number % 2 == 0;
			}
		});
		List<Integer> list = evenNumbers.collect();
		for(int number: list) {
			System.out.println(number);
		}
		context.close();
	}
	
	public static void groupByKey() {
		SparkConf conf = new SparkConf().setAppName("groupByKey tranformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
															new Tuple2<String, Integer>("class2", 70),
															new Tuple2<String, Integer>("class1", 90),
															new Tuple2<String, Integer>("class2", 65));
		JavaPairRDD<String, Integer> classScores = context.parallelizePairs(scores);
		JavaPairRDD<String, Iterable<Integer>> classScore = classScores.groupByKey();
		classScore.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){
			@Override
			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				
				System.out.println(tuple._1 + " is " + tuple._2);
				
			}
		});
		context.close();
	}
	
	public static void reduceByKey() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey tranformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
															new Tuple2<String, Integer>("class2", 70),
															new Tuple2<String, Integer>("class1", 90),
															new Tuple2<String, Integer>("class2", 65));
		JavaPairRDD<String, Integer> classScores = context.parallelizePairs(scores);
		JavaPairRDD<String, Integer> classScore = classScores.reduceByKey(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer number1, Integer number2) throws Exception {
				// TODO Auto-generated method stub
				return number1 + number2;
			}
		});
		
		classScore.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> classObject) throws Exception {
				System.out.println(classObject._1 + " total score is " + classObject._2);
			}
			
		});
		context.close();
	}
	
	public static void sortByKey() {
		SparkConf conf = new SparkConf().setAppName("sortByKey tranformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
															new Tuple2<String, Integer>("class2", 70),
															new Tuple2<String, Integer>("class1", 90),
															new Tuple2<String, Integer>("class2", 65));
		JavaPairRDD<String, Integer> classScores = context.parallelizePairs(scores);
		JavaPairRDD<String, Integer> classScore = classScores.sortByKey();
		classScore.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1 + " has score " + tuple._2);
				}
			});
	}
	
	public static void join() {
		SparkConf conf = new SparkConf().setAppName("join tranformation").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"),
				new Tuple2<Integer, String>(4, "chris"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 50),
				new Tuple2<Integer, Integer>(2, 10),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(4, 100),
				new Tuple2<Integer, Integer>(4, 120));
		
		JavaPairRDD<Integer, String> studentRdd = context.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scoreLRdd = context.parallelizePairs(scoreList);
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = studentRdd.join(scoreLRdd);
		studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>(){

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
				System.out.println("id is " + tuple._1);
				System.out.println("name is " + tuple._2._1);
				System.out.println("score is " + tuple._2._2);
			}
		});
		
	}
}
