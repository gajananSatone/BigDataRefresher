package gajanans.SparkJavaAnalytics;

//import org.apache.spark.Accumulator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAccumulator;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import gajanans.SparkJavaAnalytics.env.*;

public class SparkNodeComunication {

	public static void main(String[] args) {
		SparkJavaUtil myUtil = new SparkJavaUtil();
		JavaSparkContext jsc = myUtil.createSparkContext("Node comunication ");

		JavaRDD<String> input = jsc.textFile(HadoopDataSources.longText);
		String resultFile = HadoopDataSources.results +"LongText_BlankRemoved_"+ myUtil.getDirTimeSfix(); 

		final Accumulator<Integer> blankLines = jsc.accumulator(0);

		JavaRDD<String> callSign = input.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterator<String> call(String line) throws Exception {
						if (line.equals("")){
							blankLines.add(1);
						}
						return Arrays.asList(line.split(" ")).iterator();
					}	
				});
		callSign.saveAsTextFile(resultFile);
		System.out.println("Blank lines - " + blankLines.value());
	}

}
