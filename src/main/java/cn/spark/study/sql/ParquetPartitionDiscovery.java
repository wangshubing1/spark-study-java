package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Parquet数据源之自动推断分区
 * @author Administrator
 *
 */
public class ParquetPartitionDiscovery {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("GenericLoadSave")
				.getOrCreate();
		
		Dataset usersDF = spark.read().parquet(
				"hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
		usersDF.printSchema();
		usersDF.show();
	}
	
}
