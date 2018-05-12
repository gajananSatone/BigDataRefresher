package gajanans.HadoopPOC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}

	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		System.out.println("Reducer cleanup");
		super.cleanup(context);
	}

	@Override
	public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context arg0)
			throws IOException, InterruptedException {
		System.out.println("Reducer run");
		super.run(arg0);
	}

	@Override
	protected void setup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		System.out.println("Reducer Setup");
		super.setup(context);
	}

}
