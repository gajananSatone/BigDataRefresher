package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession

object FilterMapaPartitonComparision {

	def main(args: Array[String]): Unit = {
			val spark = SparkSession.builder()
			.appName("Compare filter and mappartition function ")
			.master("local")
			.getOrCreate()
					val sc = spark.sparkContext
					val tranFile = sc.textFile("hdfs:///user//data/ch04_data_transactions.txt")

					val filteredRDD = tranFile.filter { x => filterMyRecord(x) }

			val mapPartitonedRDD = tranFile.mapPartitions(processPartition(_), true)

					filteredRDD.collect()
//					mapPartitonedRDD.collect()

					//2015	
	}
	
	def processPartition(partiontData : Iterator[String] ): Iterator[String] = {
			print("< processPartition >")
			return  List("", "", "").iterator

	}

	def filterMyRecord (line : String) : Boolean = {
			println("< filterMyRecord >")
			return false;
	}
}