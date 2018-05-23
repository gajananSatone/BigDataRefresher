package gajanans.spark.EngineersGuide

object RunEngineering {

	def main(args: Array[String]): Unit = {
			val spark = DataEngineering.getSparkSession();

			val myRange = spark.range(1000).toDF("number")
					val divisBy2 = myRange.where("number % 2 = 0")

					val count = divisBy2.count();
			//			println(count);

			val flightData2015 = spark
					.read
					.option("inferSchema", "true")
					.option("header", "true")
					.csv("hdfs:///user//SparkData/flight-data/csv/2015-summary.csv")

					flightData2015.sort("count").explain()
					spark.conf.set("spark.sql.shuffle.partitions", "5")

					flightData2015.sort("count").take(2)
					flightData2015.createOrReplaceTempView("flight_data_2015")

					val sqlWay = spark.sql("""
							SELECT DEST_COUNTRY_NAME, count(1)
							FROM flight_data_2015
							GROUP BY DEST_COUNTRY_NAME
							""")

							val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

							sqlWay.explain
							dataFrameWay.explain
							spark.sql("SELECT max(count) from flight_data_2015").take(1)

							import org.apache.spark.sql.functions.max
							flightData2015.select(max("count")).take(1)

							val maxSql = spark.sql("""
									SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
									FROM flight_data_2015
									GROUP BY DEST_COUNTRY_NAME
									ORDER BY sum(count) DESC
									LIMIT 5
									""")		

									import org.apache.spark.sql.functions.desc
									flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count")
									.withColumnRenamed("sum(count)", "destination_total")
									.sort(desc("destination_total"))
									.limit(5)
									.collect()		

	}
}