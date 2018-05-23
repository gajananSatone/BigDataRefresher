package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession
import javax.ws.rs.GET
import org.apache.spark.util.AccumulatorV2

object CustomAccumulators {

	def main(args: Array[String]): Unit = {

			val spark = SparkSession.builder().appName(" Custom ACCUMULATORS").master("local").getOrCreate()
					val sc = spark.sparkContext
					val rdd = sc.parallelize(1 to 100)

					import org.apache.spark.AccumulableParam

					implicit object AvgAccParam extends AccumulableParam[(Int, Int),Int]{
				def zero(v:(Int, Int)) = (0, 0)
						def addInPlace(v1:(Int, Int), v2:(Int, Int)) = (v1._1+v2._1, v1._2+v2._2)
						def addAccumulator(v1:(Int, Int), v2:Int) = (v1._1+1, v1._2+v2)

						//						def add(v: (Int, Int)): Unit = ??? 
						//								def copy(): org.apache.spark.util.AccumulatorV2[(Int, Int),Int] = ??? 
						//										def isZero: Boolean = ??? 
						//												def merge(other: org.apache.spark.util.AccumulatorV2[(Int, Int),Int]): Unit = ??? 
						//														def reset(): Unit = ??? 
						//																def value: Int = ???
			}

			val acc = sc.accumulable((0,0))
					rdd.foreach ( x => acc += x )
					val mean = acc.value._2.toDouble / acc.value._1
					println("Result " + mean)

					import scala.collection.mutable.MutableList

					val colacc = sc.accumulableCollection(MutableList[Int]())
					rdd.foreach(x => colacc += x)

					val value = colacc.value
					colacc.value.foreach (println(_))	
	}
}