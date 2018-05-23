package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession

object SparkApiInDepth {

	// home///GitProjects/spark-in-action/ch04
	// Improvise this code to use CaseClass
	// use parameter for % discount 


	def main(args: Array[String]): Unit = {
	
	  val spark = SparkSession.builder().appName("SparkApiInDepth").getOrCreate()
					val sc = spark.sparkContext

					val tranFile = sc.textFile("/user//data/ch04_data_transactions.txt")
					
//					val partitionedMappedTran = tranFile.mapPartitions(x => processPartition(x), false)
					
					
					val tranData = tranFile.map (_.split("#"))
					var transByCust = tranData.map (tran => (tran(2).toInt , tran))
					//					transByCust.keys.distinct().count() // 100

//					val testCoalesce = sc.parallelize(Range(1,1000, 10) , 10)
//					testCoalesce.partitions.length
//					
//					val testCoalesce1 = testCoalesce.coalesce(20, true)
//					val testCoalesce1 = testCoalesce.coalesce(20)					
//					val testCoalesce2 = testCoalesce.repartition(20)
					
					transByCust.countByKey()
					transByCust.countByKey().values.sum

					val (cid , purch) = transByCust.countByKey().toSeq.sortBy(_._2).last

					var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4","1", "0.00"))
					transByCust.lookup(53).foreach (tran => println(tran.mkString(" , ")))

					transByCust = transByCust.mapValues (tran => {
						if (tran(3).toInt == 25 && tran(4).toDouble > 1 )
							tran(5) = (tran(5).toDouble * 0.95).toString()
							tran
					})
					
					transByCust = transByCust.flatMapValues (tran => {
					  if (tran(3).toInt == 81 && tran(4).toDouble >= 5 ){
					    val cloned = tran.clone()
					    cloned(5) = "0.00";
					    cloned(3) = "70";
					    cloned(4) = "1";
					    List(tran, cloned)
					  }
					  else {
					    List(tran)
					  }
					})
					
					val amounts = transByCust.mapValues(t => t(5).toDouble)
					val totals = amounts.foldByKey(0)((p1 , p2) => p1 + p2).collect()
					totals.toSeq.sortBy(_._2).last
					totals.toSeq.foreach(tran => { println ( tran._1 + " , " +  tran._2) })
					
//					amounts.foldByKey(100000)((p1 , p2) => p1 + p2).collect()
					
					complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76","63", "1", "0.00")
					
					transByCust = transByCust.union(sc.parallelize(complTrans).map (t => (t(2).toInt, t)))
					
					transByCust.map(t => t._2.mkString("#")).saveAsTextFile("ch04output-transByCust")
				
//				val prods1 = transByCust.aggregateByKey(zeroValue)(seqOp, combOp)
					
					val prods = transByCust.aggregateByKey(List[String]())(
					    (prods , tran) => prods ::: List(tran(3)),
					    (prods1, prods2) => prods1 ::: prods2)
					    
					transByCust.partitions.size  
					
					
								def createComb = (t:Array[String]) => {
				val total = t(5).toDouble
						val q = t(4).toInt
						(total/q, total/q, q , total)			  
			}

			def meargeVal : ((Double, Double, Int, Double), Array[String]) => (Double, Double, Int, Double) = { 
			case ((mn , mx , c , tot) ,t ) => {
				val total = t(5).toDouble
						val q = t(4).toInt
						(scala.math.min(mn,total/q),scala.math.max(mx,total/q),c+q,tot+total)
			}	}

			def mergeComb:((Double,Double,Int,Double),(Double,Double,Int,Double))=> (Double,Double,Int,Double) = {
			case((mn1,mx1,c1,tot1),(mn2,mx2,c2,tot2)) => (scala.math.min(mn1,mn1),scala.math.max(mx1,mx2),c1+c2,tot1+tot2)
			}
			
			val avgByCust = transByCust.combineByKey(createComb, meargeVal, mergeComb, new org.apache.spark.HashPartitioner(transByCust.partitioner.size))
			.mapValues({case(mn,mx,cnt,tot) => (mn,mx,cnt,tot,tot/cnt)})
			
	}
	
	def processPartition(partiontData : Iterator[String] ): Iterator[String] = {
	  return  List("", "", "").iterator
	}

}