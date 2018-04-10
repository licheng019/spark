package SparkSqlandDataFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DataFrameCreate {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		Dataset<Row> df = sqlContext.read().json("students.json");
		df.show();
		df.printSchema();
		df.select("name").show();
		//df.select(df("name"), df("age") + 1).show();
		df.groupBy("18").count().show();
	}
}
