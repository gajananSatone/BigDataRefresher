package gajanans.spark.hdpapparchitect

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import scala.util.Random


object GenDedupInput {

  def main(args: Array[String]): Unit = {

			if (args.length < 3){
				println("{outputPath} {numberOfRecords} {numberOfUniqueRecords}")
				return
			}

			genDedupFile(args)
	}

	def genDedupFile(args: Array[String]){

		val outPutPath = new Path(args(0))
		// The number of records to be written to the file
		val numberOfRecords = args(1).toInt
		//Number of unique primary key
		val numberOfUniqueRecords = args(2).toInt

		// Open filesystem to HDFS
		val fileSystem = FileSystem.get(new Configuration)

		val writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outPutPath)))

		val r = new Random()

		for (i <- 0 to numberOfRecords){
			val uniqueId = r.nextInt(numberOfUniqueRecords)
					// Format: {key}, {timeStamp}, {value}
					writer.write(uniqueId + "," + i + "," + r.nextInt(10000))
					writer.newLine()
		}
		writer.close()
	}

}
