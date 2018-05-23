package gajanans.spark.EngineersGuide

import org.apache.spark.sql.SparkSession

object DFCaching {
	def main(args: Array[String]): Unit = {
			val spark = SparkSession.builder().master("local").appName("DF caching").enableHiveSupport().getOrCreate();

			val DF1 = spark.read
			.format("csv")
			.option("inferSchema", "true")
			.option("header","true")
			.load("hdfs:///user//SparkData/flight-data/csv/2015-summary.csv")
			DF1.cache()
			val DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
			val DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
			val DF4 = DF1.groupBy("count").count().collect()
			
	}
}