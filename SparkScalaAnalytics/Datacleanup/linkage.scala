package gajanans.spark.Datacleanup

import org.apache.spark.SparkContext
import java.lang.Double.isNaN
object linkage {

	def main(args: Array[String]): Unit = {

			val sc = new SparkContext("local", "Interview hands on")

			val rawblocks = sc.textFile("hdfs:///gajanans/linkage")
			val head = rawblocks.take(10)
//		head.filterNot(isHeader).foreach (println)
//		head.filter(x => !isHeader(x)).foreach (println)
      val noheader = rawblocks.filter (x => !isHeader(x))
   
      val parsed = noheader.map { line => parse(line) }
		
			
			parsed.cache()
			
	
			
//		val grouped = parsed.groupBy( md => md.matched )
			
			// Scala array processing 
			val mds = head.filter(x => !isHeader(x)).map(x => parse(x))
			val grouped = mds.groupBy { md => md.matched }
			grouped.mapValues ( x => x.size ).foreach(println)
			
			//Spark RDD functions
			val matchCounts = parsed.map (md => md.matched ).countByValue()
      val matchCountsSeq = matchCounts.toSeq
      matchCountsSeq.sortBy(_._1).foreach(println)
      matchCountsSeq.sortBy(_._2).reverse.foreach(println)
      
      parsed.map ( md => md.rawscores(0) ).filter(!isNaN(_)).stats()
      val stats = (0 until 9).map(i => parsed.map ( md => md.rawscores(i) ).filter(!isNaN(_)).stats())
      
      val nasRDD = parsed.map (md => {
        md.rawscores.map(d => NAStatCounter(d))
        })
        
      val reduced = nasRDD.reduce((n1 , n2) => {
        n1.zip(n2).map{ case (a , b) => a.merge(b) }
      })
      
      reduced.foreach (println)
 	}
	
	def parse(line : String) = {
	    val pieces = line.split(",")
      val id1= pieces(0).toInt
      val id2= pieces(1).toInt
      val rawscores = pieces.slice(2, 11).map { toDouble }
	    val matched= pieces(11).toBoolean
	    MatchedData(id1 , id2 , rawscores , matched)    
	}
	
	def toDouble(s: String) = {  if ("?".equals(s)) Double.NaN else s.toDouble	}
	
	def isHeader(line : String) : Boolean = {
	  line.contains("id_1")
	}
	
	case class MatchedData( id1 : Int, id2 : Int, rawscores:Array[Double], matched: Boolean )
	
}