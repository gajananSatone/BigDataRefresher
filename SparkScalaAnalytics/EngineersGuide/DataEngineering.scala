package gajanans.spark.EngineersGuide

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object DataEngineering {

	def getSparkSession() : SparkSession =  {
		val spark = SparkSession
				.builder().master("local")
				.appName("Data Engineeiring")
				.enableHiveSupport()
				.getOrCreate()
				
				return spark;
	}

	def toDouble(s: String) = { if ("?".equals(s)) Double.NaN else s.toDouble}

	def naz(d:Double) = if (Double.NaN.equals(d)) 0.0 else d

}