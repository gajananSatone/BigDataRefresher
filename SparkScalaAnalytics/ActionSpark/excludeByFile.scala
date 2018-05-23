package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object excludeByFile {

	def main(args: Array[String]): Unit = {

			// parameterize appName , appMaster , inputPath , and empPath
			val env = new SparkEnvUTIL()

			val spark = SparkSession.builder()
			.appName("GitHub Excluding non-employees")
			.master("local[*]")
			.getOrCreate()


			val sc = spark.sparkContext
			val inputPath = env.HADOPP_HOST + "/user//data/github-archive/2015-03-01-0.json"
			val ghLog = spark.read.json(inputPath)

			val homeDir = System.getenv("HOME")

			//expose empPath as app. param

			val empPath  = "/home///GitProjects/first-edition/ch03/ghEmployees.txt"
			val pushes = ghLog.filter("type = 'PushEvent'")
			val grouped = pushes.groupBy("actor.login").count()
			val ordered = grouped.orderBy(grouped("count").desc)

			val employees = Set() ++ (
					for {
						line <- fromFile(empPath).getLines() 
					} yield line.trim   
					)
					val bcEmployees = sc.broadcast(employees)
					val isEmp = user => bcEmployees.value.contains(user)
					val isEmployee = spark.udf.register("isEmpUdf", isEmp)

					import spark.implicits._

					val filtered = ordered.filter(isEmployee($"login")) 

					filtered.show()
	}
}