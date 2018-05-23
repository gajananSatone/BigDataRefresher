package gajanans.spark.ActionSpark

object ActionIntro {
  
	def main(args: Array[String]): Unit = {
			
	    val sparkENV = new SparkEnvUTIL()
			val spark = sparkENV.createSparkSession("Spark wordcount", "local")
			val file = spark.sparkContext.textFile("hdfs://...")
			val counts = file.flatMap (line => line.split(" ")).map(word => (word, 1)).countByKey()
					
					//					counts.saveAsTextFile("hdfs://...")
			val sc = spark.sparkContext
			val numbers = sc.makeRDD(10 to 50 by 10)
			
			val numbersqured = numbers.map ( num => num * num )
			
			var inputFfile = "/data/client-ids.log"
			val lines = sc.textFile("/data/client-ids.log")
			
			val idsStr= lines.map ( line => line.split(",") )
			val ids= lines.flatMap ( _.split(",") )
			val intIds= ids.map (_.toInt)
			val uniqueIds = intIds.distinct
			
			val s = uniqueIds.sample(false,0.3)
			s.collect()
			intIds.mean()
			intIds.histogram(Array(1.0, 50.0, 100.0))
			intIds.histogram(3)
			
			
			
	}
}
