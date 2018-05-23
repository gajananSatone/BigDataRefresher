package gajanans.spark.hdpapparchitect
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import scala.util.Random
import java.net.URI

object SparkPeaksAndValleysDataGen {
  
	def main(args: Array[String]): Unit = {
			if (args.length == 0) {
				println("{outputPath} {numberOfRecords} {numberOfUniqueIds}")
				return
			}

			val outputPath = new Path(args(0))
			val numberOfRecords = args(1).toInt
			val numberOfUniqueIds = args(2).toInt

			val fileSystem = FileSystem.get(URI.create(args(0)), new Configuration)
			
			val writer =	new BufferedWriter( new OutputStreamWriter(fileSystem.create(outputPath)))
			val r = new Random()
			
			var direction = 1
			var directionCounter = r.nextInt(numberOfUniqueIds * 10)
			var currentPointer = 0
			
			for (i <- 0 until numberOfRecords) {
				val uniqueId = r.nextInt(numberOfUniqueIds)
						currentPointer = currentPointer + direction
						directionCounter = directionCounter - 1
						if (directionCounter == 0) {
							var directionCounter = r.nextInt(numberOfUniqueIds * 10)
									direction = direction * -1
						}
				writer.write(uniqueId + "," + i + "," + currentPointer)
				writer.newLine()
			}
			writer.close()
	}


}