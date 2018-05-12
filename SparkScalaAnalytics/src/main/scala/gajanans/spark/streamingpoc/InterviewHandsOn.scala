package gajanans.spark.streamingpoc

import org.apache.spark.SparkContext

object InterviewHandsOn {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Interview hands on")
    val a = sc.parallelize(1 to 9, 3)
    a.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect
     println(a.partitioner)
    
    val b = sc.parallelize(1 to 20, 3)
    b.groupBy (x => { if (x < 5) "UpTO5" else if (x > 5 & x <=10) "5TO10" else if (x > 10 & x <=15) "10TO15" else "15TO20" }).collect()
    
//    val aa = sc.parallelize(1 to 10, 3)
//    val ba = sc.parallelize(11 to 16, 3)
//    aa.zip(ba).collect
//    
//    val bb = sc.parallelize(1 to 6, 3)
//    val bb1 = sc.parallelize(3 to 6, 3)
//    bb.subtract(bb1).collect()
//    bb1.subtract(bb).collect()
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.sql("SELECT * FROM parquet.`hdfs://localhost:54310/gajanans/userdata1.parquet`")
    df.show()
    df.select("email").show()
  }
}