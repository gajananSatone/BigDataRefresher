import org.apache.spark.SparkContext

object IInitiateContext {
	def main(args: Array[String]): Unit = {
			val sc = new SparkContext("local", "Test RDD API")
			val aaa = sc.parallelize(0 to 9, 3)
//			val zipped = aaa.zipPartitions(bbb, ccc)( myZip)
			val zz = sc.parallelize(Array("A", "B", "C", "D"))
			val r = zz.zipWithIndex()
			val rr = zz.zipWithUniqueId() 
			
//			zz.collect().foreach { x => println(x) }
			val file = sc.textFile("hdfs://localhost:54310/gajanans/last_fm/user_artist_data.txt")
			println(file.getNumPartitions)

			
	}
}