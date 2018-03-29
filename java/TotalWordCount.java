import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class TotalWordCount {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TotalWordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> lines = context.textFile("wordCountExample");
		JavaRDD<Integer> line = lines.map(new Function<String, Integer>(){
			@Override
			public Integer call(String line) throws Exception {
				// TODO Auto-generated method stub
				return line.length();
			}
		});
		
		Integer counts = line.reduce(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer number1, Integer number2) throws Exception {
				// TODO Auto-generated method stub
				return number1 + number2;
			}
			
		});
		System.out.println(counts);
	}
}

