package com.chengli.spark.ex.sparkCore;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SortWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile("wordCountExample");
		JavaRDD<String> line = lines.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterator<String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		JavaPairRDD<String, Integer> singlewordCount = line.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> wordCount = singlewordCount.reduceByKey(new Function2<Integer, Integer, Integer>(){
			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				// TODO Auto-generated method stub
				return num1 + num2;
			}
		});
		
		JavaPairRDD<Integer, String> numberWordPair = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>(){

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}
		});
		
		JavaPairRDD<Integer, String> sortNumberWordPair = numberWordPair.sortByKey(false);
		JavaPairRDD<String, Integer> sortWordCount = sortNumberWordPair.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(tuple._2, tuple._1);
			}	
		});
		
		sortWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println(tuple._1 + " appeared " + tuple._2);
			}
		});
		
		
	}
}
