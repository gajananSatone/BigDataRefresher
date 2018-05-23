package gajanans.spark.hdpapparchitect

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextInputFormat

object SparkDedupExecution {

	def main(args: Array[String]): Unit = {

			if (args.length < 2) {
				println("{inputPath} {outputPath}")
				return
			}

			val sparkConf = new SparkConf().setAppName("SparkDedupExecution")
					sparkConf.set("spark.cleaner.ttl", "120000")
					val spark = SparkSession.builder()
					.config(sparkConf)
					.master("local")
					.getOrCreate()
					val sc = spark.sparkContext

					val inputPath = args(0)
					val outputPath = args(1)

					// Read data in from HDFS
					val dedupOriginalDataRDD = sc.hadoopFile(inputPath,
							classOf[TextInputFormat],
							classOf[LongWritable],
							classOf[Text],
							1)

							// Get the data in a key-value format
							val keyValueRDD = dedupOriginalDataRDD.map(t => {
								val splits = t._2.toString.split(",")
										(splits(0), (splits(1), splits(2)))
							})

							// reduce by key so we will only get one record for every primary key
							val reducedRDD = keyValueRDD.reduceByKey((a,b) => if (a._1.compareTo(b._1) > 0) a else b)			

							// Format the data to a human-readable format and write it back out to HDFS
							reducedRDD
							.map(r => r._1 + "," + r._2._1 + "," + r._2._2)
							.saveAsTextFile(outputPath)

	}
}