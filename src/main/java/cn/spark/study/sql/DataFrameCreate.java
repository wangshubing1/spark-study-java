package cn.spark.study.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 使用json文件创建DataFrame
 * @author Administrator
 *
 */
public class DataFrameCreate {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("DataFrameCreate")
				.getOrCreate();
		
		Dataset df = spark.read().json("hdfs://spark1:9000/students.json");
		
		df.show();  
	}
	
}
