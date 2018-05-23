package gajanans.spark.ActionSpark

import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.Row

object SparklingQueries {
	def main(args: Array[String]): Unit = {

			val spark = SparkSession.builder().appName("Sparkling Queries").master("local").getOrCreate()
					val sc = spark.sparkContext
					val itPostsRows = sc.textFile("hdfs:///user//data/italianPosts.csv")

					// CONVERTING RDDS TO DATAFRAMES BY SPECIFYING A SCHEMA

					import org.apache.spark.sql.types._
					val postSchema = StructType(
							Seq(
									StructField("commentCount", IntegerType, true),
									StructField("lastActivityDate", TimestampType, true),
									StructField("ownerUserId", LongType, true),
									StructField("body", StringType, true),
									StructField("score", IntegerType, true),
									StructField("creationDate", TimestampType, true),
									StructField("viewCount", IntegerType, true),
									StructField("title", StringType, true),
									StructField("tags", StringType, true),
									StructField("answerCount", IntegerType, true),
									StructField("acceptedAnswerId", LongType, true),
									StructField("postTypeId", LongType, true),
									StructField("id", LongType, false)
									)
							)

							import spark.implicits._
							val rowRDD= itPostsRows.map (row => stringToRow(row))
							val itPostDFStruct = spark.createDataFrame(rowRDD, postSchema)

							val postsDf = itPostDFStruct

							// SELECTING DATA
							val postsIdBody = postsDf.select("id", "body")
							val postsIdBody1 = postsDf.select(postsDf.col("id"), postsDf.col("body"))
							val postsIdBody2 = postsDf.select(Symbol("id"), Symbol("body"))
							val postsIdBody3 = postsDf.select('id, 'body)
							val postsIdBody4 = postsDf.select($"id", $"body")
							
							// Dropping perticuler column in new dataframe
							val postIds = postsIdBody.drop("body")
							
							
							//FILTERING DATA
							
							postsIdBody.filter('body contains "Italiano").count
							val noAnswer = postsDf.filter(('postTypeId === 1) and ('acceptedAnswerId isNull))
							val firstTenQs = postsDf.filter('postTypeId === 1).limit(10)
							val firstTenQs1 = firstTenQs.withColumnRenamed("ownerUserId", "owner")
							
//							firstTenQs1.show()
							
							postsDf.filter('postTypeId === 1).
							withColumn("ratio", 'viewCount / 'score).
							where('ratio < 35).show()
	}  

	def createDFTypes(spark : SparkSession){

		val sc = spark.sparkContext
				import spark.implicits._

				val itPostsRows = sc.textFile("hdfs:///user//data/italianPosts.csv")
				val itPostsSplit = itPostsRows.map (line => line.split("~"))

				// CREATING A DATAFRAME FROM AN RDD OF TUPLES
				val itPostsRDD = itPostsSplit.map (x => (x(0), x(1), x(2), x(3),x(4),x(5), x(6), x(7), x(8),x(9),x(10), x(11), x(12)))
				val itPostsDFrame = itPostsRDD.toDF("commentCount", "lastActivityDate","ownerUserId", "body", "score", "creationDate", "viewCount", "title","tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")

				itPostsDFrame.show(10)
				itPostsDFrame.printSchema()

				// CONVERTING RDDS TO DATAFRAMES USING CASE CLASSES

				val itPostDFCase = itPostsRows.map (x => stringToPost(x)).toDF()
				itPostDFCase.printSchema()


				// CONVERTING RDDS TO DATAFRAMES BY SPECIFYING A SCHEMA

				import org.apache.spark.sql.types._
				val postSchema = StructType(
						Seq(
								StructField("commentCount", IntegerType, true),
								StructField("lastActivityDate", TimestampType, true),
								StructField("ownerUserId", LongType, true),
								StructField("body", StringType, true),
								StructField("score", IntegerType, true),
								StructField("creationDate", TimestampType, true),
								StructField("viewCount", IntegerType, true),
								StructField("title", StringType, true),
								StructField("tags", StringType, true),
								StructField("answerCount", IntegerType, true),
								StructField("acceptedAnswerId", LongType, true),
								StructField("postTypeId", LongType, true),
								StructField("id", LongType, false)
								)
						)

						val rowRDD= itPostsRows.map (row => stringToRow(row))
						val itPostDFStruct = spark.createDataFrame(rowRDD, postSchema)
						itPostDFStruct.printSchema()

						itPostsDFrame.columns
						itPostsDFrame.dtypes

						itPostDFCase.columns
						itPostDFCase.dtypes

						itPostDFStruct.columns
						itPostDFStruct.dtypes
	}

	import StringImplicits._

	def stringToPost(row:String):Post = {
			val r = row.split("~")
					Post(r(0).toIntSafe,
							r(1).toTimestampSafe,     
							r(2).toLongSafe,
							r(3),
							r(4).toIntSafe,
							r(5).toTimestampSafe,
							r(6).toIntSafe,
							r(7),
							r(8),
							r(9).toIntSafe,
							r(10).toLongSafe,
							r(11).toLongSafe,
							r(12).toLong  )

	}

	def stringToRow(row:String):Row = {
			val r = row.split("~")
					Row(r(0).toIntSafe.getOrElse(null),
							r(1).toTimestampSafe.getOrElse(null),     
							r(2).toLongSafe.getOrElse(null),
							r(3),
							r(4).toIntSafe.getOrElse(null),
							r(5).toTimestampSafe.getOrElse(null),
							r(6).toIntSafe.getOrElse(null),
							r(7),
							r(8),
							r(9).toIntSafe.getOrElse(null),
							r(10).toLongSafe.getOrElse(null),
							r(11).toLongSafe.getOrElse(null),
							r(12).toLong  
							)
	}


case class Post(
		commentCount:Option[Int],
		lastActivityDate:Option[java.sql.Timestamp],
		ownerUserId:Option[Long],
		body:String,
		score:Option[Int],
		creationDate:Option[java.sql.Timestamp],
		viewCount:Option[Int],
		title:String,
		tags:String,
		answerCount:Option[Int],
		acceptedAnswerId:Option[Long],
		postTypeId:Option[Long],
		id:Long
		)
}

object StringImplicits {
	implicit class StringImprovements(val s: String) {

		import scala.util.control.Exception.catching

		def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
		def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
		def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
	}
}
