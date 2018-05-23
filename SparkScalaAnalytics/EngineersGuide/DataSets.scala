package gajanans.spark.EngineersGuide

object DataSets {

	def main(args: Array[String]): Unit = {


			val spark = DataEngineering.getSparkSession();
//	  	import org.apache.spark._

//	  spark.range(2000).map(value => ValueAndDouble(value, value * 2)).filter(vAndD => vAndD.valueDoubled % 2 == 0).where("value % 3 = 0").count()
	}
	
	case class ValueAndDouble(value:Long, valueDoubled:Long)
}
