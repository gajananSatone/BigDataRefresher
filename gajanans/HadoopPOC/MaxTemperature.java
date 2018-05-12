package gajanans.HadoopPOC;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		HadoopUTIL hdpUtil = new HadoopUTIL();
		String outPutFileName = args[1] + "/"+ hdpUtil.getDirTimeSfix() + "/";
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temprature");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(outPutFileName));

		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
