package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession

object HandsOn {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("HandsOnPractice").master("local").getOrCreate()
    val sc = spark.sparkContext
    
    val list1 = List.fill(500)(scala.util.Random.nextInt(100))
    val myRdd = sc.parallelize(list1, 30).glom()
    
    
    // RDD Dependencies
    val list = List.fill(500)(scala.util.Random.nextInt(10))
    val listRdd = sc.parallelize(list,5)
    val pairs = listRdd.map (x => (x, x*x))
    val reduced = pairs.reduceByKey((v1, v2) => v1 + v2)
    val finalRdd = reduced.mapPartitions(iter => iter.map({case(k,v) => "K = "+ k + " , V = " + v}))
    finalRdd.collect()
    
//    val list1 = List.fill(100)(getListItem)
  }
  
  def getListItem(){
    
  }
}