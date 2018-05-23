package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession
import scala.math.Ordered
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.math.Ordering

object SparkJoiningSortingGrouping {

	def main(args: Array[String]): Unit = {
			val spark = SparkSession
					.builder()
					.appName("SparkApiInDepth")
					.getOrCreate()
					val sc = spark.sparkContext

					val tranFile = sc.textFile("hdfs:///user//data/ch04_data_transactions.txt")

					val tranData = tranFile.map (_.split("#"))

					val transByPprod = tranData.map (tran => (tran(3).toInt , tran))
					val totalsByProd = transByPprod
					.mapValues (t => t(5)
					.toDouble)
					.reduceByKey{case (tot1 , tot2 ) => tot1 + tot2 }

			val products = sc.textFile("hdfs:///user//data/ch04_data_products.txt")
					.map (line => line.split("#"))
					.map (p => (p(0).toInt, p))
					val totalsAndProds = totalsByProd.join(products)
					totalsAndProds.first()

					val totalsWithMissingProds = totalsByProd.leftOuterJoin(products)
					val missingProd =totalsWithMissingProds.filter(x => x._2._1 == None).map(x => x._2._2)
					missingProd.foreach ( p => println(p.mkString(" , ")) )			

					val missingProdUsingSubbtract = products.subtractByKey(totalsByProd).values
					missingProdUsingSubbtract.foreach ( p => println(p.mkString(" , ")) )

					val prodTotCogroup = totalsByProd.cogroup(products)
					prodTotCogroup.filter(x => x._2._1.isEmpty).foreach(x => println(x._2._2.head.mkString(", ")))

					//--------------------------------------------------------------
					// Comparing two RDD using cartesian
					//--------------------------------------------------------------
					val rdd1 = sc.parallelize(List(7,8,9))
					val rdd2 = sc.parallelize(List(1,2,3))
					rdd1.cartesian(rdd2).filter(el => el._1 % el._2 == 0).collect()

					val rdd11 = sc.parallelize(1 to 10, 10)
					val rdd22 = sc.parallelize((1 to 8).map(x=>"n"+x), 10)


					rdd11.zipPartitions(rdd22, true)((iter1, iter2) => {
						iter1.zipAll(iter2, -1, "empty")
						.map({case(x1, x2)=>x1+"-"+x2})
					}).collect()

					// Sorting 

					val sortedProds = totalsAndProds.sortBy(_._2._2(1))
					sortedProds.collect()

					//val sortedProdsPartition = totalsAndProds.repartitionAndSortWithinPartition()

					implicit val emplOrdering = new Ordering[Employee]{
				override def compare(a: Employee, b: Employee) =
						a.lastName.compare(b.lastName)
			}


	}

case class Employee (lastName: String) extends Ordered[Employee]{
		override def compare(that: Employee) =  this.lastName.compare(that.lastName) 
	}

}