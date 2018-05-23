package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession

object archiveAnalyzer {

	def main(args: Array[String]): Unit = {

			val env = new SparkEnvUTIL()

			val spark = SparkSession.builder()
			.appName("GitHub push counter")
			.master("local[*]")
			.getOrCreate()

			val sc = spark.sparkContext
			val inputPath = env.HADOPP_HOST + "/user//data/github-archive/2015-03-01-0.json"
			val ghLog = spark.read.json(inputPath)
			val pushes = ghLog.filter("type = 'PushEvent'")

//			pushes.printSchema()
			println("all events: " + ghLog.count)
			println("only pushes: " + pushes.count)
//			pushes.show(5)
			
			val grouped = pushes.groupBy("actor.login").count()
			grouped.show(5)
			
			val ordered = grouped.orderBy(grouped("count").desc)
			ordered.show(5)
			
			
	}
}  