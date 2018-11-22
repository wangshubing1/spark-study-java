package cn.spark.study.sql;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;


/**
 * Parquet数据源之使用编程方式加载数据
 * @author Administrator
 *
 */
public class ParquetLoadData {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("GenericLoadSave")
				.getOrCreate();
		
		// 读取Parquet文件中的数据，创建一个DataFrame
		Dataset usersDF = spark.read().parquet(
				"hdfs://spark1:9000/spark-study/users.parquet");
		
		// 将DataFrame注册为临时表，然后使用SQL查询需要的数据
		usersDF.registerTempTable("users");
		Dataset userNamesDF = spark.sql("select name from users");
		
		// 对查询出来的DataFrame进行transformation操作，处理数据，然后打印出来
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> userNames = userNamesDF.map(
				(MapFunction<Row,String>) row -> "Name:" + row.getString(0),stringEncoder);
		userNames.show();
		/*List<String> userNames = userNamesDF.javaRDD().map(
				new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}
			
		}).collect();*/
		
		/*for(String userName : userNames) {
			System.out.println(userName);  
		}*/
	}
	
}
