package com.chengli.spark.ex.sparkCore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SecondarySort {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile("numberFile");
		JavaPairRDD<SecondarySortKey, String> sortPairdRDD = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>(){
			@Override
			public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
				// TODO Auto-generated method stub
				SecondarySortKey sortKey = new SecondarySortKey();
				String[] numbers = line.split(" ");
				sortKey.setFirst(Integer.parseInt(numbers[0]));
				sortKey.setSecond(Integer.parseInt(numbers[1]));
				
				return new Tuple2<SecondarySortKey, String>(sortKey, line);
			}
		});
		JavaPairRDD<SecondarySortKey, String> sortedPairs = sortPairdRDD.sortByKey();
		JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>(){

			@Override
			public String call(Tuple2<SecondarySortKey, String> tuple) throws Exception {
				// TODO Auto-generated method stub
				return tuple._2;
			}
		});
		sortedLines.foreach(new VoidFunction<String>() {

			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});
	}
}
