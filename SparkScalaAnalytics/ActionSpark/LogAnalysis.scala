package gajanans.spark.ActionSpark
import org.apache.spark.sql.SparkSession

import scala.io.Source.fromFile

object LogAnalysis {

	def main(args: Array[String]): Unit = {

			// parameterize appName , appMaster , inputPath , and empPath
			if (args.length < 3){

				System.err.println("""< Path to the input JSON files> 
						<Path to the employees file> 
						<Path to the output file>
						<Output format[JSON , Parquet]>""")
						System.exit(-1)
			}	

			val inputLogFile = args(0)
					val employeeFile = args(1)
					val outputFile = args(2)
					val outputFormat = args(3)

					val env = new SparkEnvUTIL()

			val spark = SparkSession.builder()
			.appName("GitHub Excluding non-employees")
			//			.master("local[*]")
			.getOrCreate()


			val sc = spark.sparkContext

			val ghLog = spark.read.json(inputLogFile)

			val homeDir = System.getenv("HOME")

			//expose empPath as app. param

			val empPath = employeeFile
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

					filtered.write.format(outputFormat).save(outputFile)
	}
}