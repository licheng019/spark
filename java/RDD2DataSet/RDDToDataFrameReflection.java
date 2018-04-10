package SparkSqlandDataFrame;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RDDToDataFrameReflection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> lines = sc.textFile("students.file");
		JavaRDD<Student> students = lines.map(new Function<String, Student>(){
			
			@Override
			public Student call(String line) throws Exception {
				String[] words= line.split(",");
				Student student = new Student();
				student.setId(Integer.parseInt(words[0]));
				student.setName(words[1]);
				student.setAge(Integer.parseInt(words[2]));
				return student;
			}
		});
		
		Dataset<Row> studentDF = sqlContext.createDataFrame(students, Student.class);
		studentDF.registerTempTable("students");
		Dataset<Row> teenagerDF = sqlContext.sql("select * from students where age <= 18");
		
		teenagerDF.show();
		JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();
		JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>(){

			@Override
			public Student call(Row row) throws Exception {
				Student student = new Student();
				student.setId(row.getInt(0));
				student.setName(row.getString(2));
				student.setAge(row.getInt(1));
				return student;
			}
		});
		teenagerStudentRDD.foreach(new VoidFunction<Student>(){

			@Override
			public void call(Student student) throws Exception {
				System.out.println(student);
			}
			
		});
	}
	
	
}
