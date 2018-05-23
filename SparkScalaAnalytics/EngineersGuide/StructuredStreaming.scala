package gajanans.spark.EngineersGuide

import org.apache.spark.sql.SparkSession

object StructuredStreaming {
	def main(args: Array[String]): Unit = {
			val spark = SparkSession.builder().master("local").appName("Structured Streaming").enableHiveSupport().getOrCreate();

			val staticDataFrame = spark.read
					.format("csv")
					.option("header", "true")
					.option("inferschema", "true")
					.load("hdfs:///user//SparkData/retail-data/by-day/*.csv")

					staticDataFrame.createOrReplaceTempView("retail_data")

					val staticSchema = staticDataFrame.schema

					import org.apache.spark.sql.functions.{window, column, desc, col}
			import spark.implicits._

			staticDataFrame.selectExpr(
					"CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
					.groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
					.sum("total_cost")
					.orderBy(desc("sum(total_cost)"))
					.take(5)

					spark.conf.set("spark.sql.shuffle.partitions", "5")

					val streamingDataFrame = spark.readStream
					.schema(staticSchema)
					.option("maxFilesPerTrigger", 1)
					.format("csv")
					.option("inferschema", "true")
					.load("hdfs:///user//SparkData/retail-data/by-day/*.csv")

					val purchaseByCustomerPerHour = streamingDataFrame
					.selectExpr(
							"CustomerId","(UnitPrice * Quantity) as total_cost","InvoiceDate")
							.groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
							.sum("total_cost")


							purchaseByCustomerPerHour.writeStream
							.format("memory") // memory = store in-memory table
							.queryName("customer_purchases") // counts = name of the in-memory table
							.outputMode("complete") // complete = all the counts should be in the table
							.start()		

							purchaseByCustomerPerHour.writeStream
							.format("console") // memory = store in-memory table
							.queryName("customer_purchases2") // counts = name of the in-memory table
							.outputMode("complete") // complete = all the counts should be in the table
							.start()		

							
							spark.sql("""SELECT * FROM customer_purchases ORDER BY `sum(total_cost)` DESC """).take(5)

	}
}