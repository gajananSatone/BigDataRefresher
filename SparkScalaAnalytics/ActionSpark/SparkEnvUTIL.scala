package gajanans.spark.ActionSpark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class SparkEnvUTIL {
 
  var HADOPP_HOST = "hdfs://"
   
	def createSparkSession (appName : String , masterIN : String) : SparkSession = {
	  var master = ""  
		if (masterIN.isEmpty()){
				master= "local"
		}else {
				master= masterIN
		}	

	  val conf = new SparkConf()
	  val spark = SparkSession
	  .builder()
	  .enableHiveSupport()
	  .appName(appName)
	  .config(conf)
	  .master("local")
	  .getOrCreate()

	  return spark;
	}


}