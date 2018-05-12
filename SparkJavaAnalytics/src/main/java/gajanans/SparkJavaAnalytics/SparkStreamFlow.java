package gajanans.SparkJavaAnalytics;
import gajanans.SparkJavaAnalytics.env.SparkJavaUtil;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

public class SparkStreamFlow {

	public static void main(String[] args) {
		SparkJavaUtil myUtil = new SparkJavaUtil();
		JavaStreamingContext jssc = myUtil.createJavaSparkStreamContext("Spark Stream Flow", 1);
		
		JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
		lines.print();
		
		JavaDStream<String> errorLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String line) {
				System.out.println("Applying filtr on " + line);
				return line.contains("error");				
			}
		});
		
		errorLines.print();		
		jssc.start();
		
		try {
			jssc.awaitTerminationOrTimeout(30000);
//			jssc.awaitTerminationOrTimeout(0);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		jssc.stop();

	}
}