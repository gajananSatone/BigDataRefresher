package gajanans.SparkJavaAnalytics;

import gajanans.SparkJavaAnalytics.env.HadoopDataSources;
import gajanans.SparkJavaAnalytics.env.SparkJavaUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class HiveConnector {

	public static void main(String[] args) {
		SparkJavaUtil myUtil = new SparkJavaUtil();
		SparkSession jspark = myUtil.createJavaSparkSession("Hive Connector");
		
		Dataset<Row> df = jspark.read().json(HadoopDataSources.testweetJason);

		df.printSchema();
		df.createOrReplaceTempView("tweets");
		df.show();
	
		Dataset<Row> result = jspark.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
		result.show();
		
		JavaRDD<String> topTweetText = df.toJavaRDD().map(new Function<Row, String>() {
			public String call(Row row) {
				return row.getString(0);
			}
			
		}) ;		

	}

}
