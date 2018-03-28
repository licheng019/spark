package com.chengli.spark.ex.sparkCore;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Word Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		//input from source
		JavaRDD<String> lines = sc.textFile("wordCountExample");
		//transformation
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
			@Override
			public Iterator<String> call(String line) throws Exception {	
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> wordCount) throws Exception {
				System.out.println(wordCount._1 + " appeared " + wordCount._2);
			}
			
		});
	}
}
