package gajanans.MRtypes;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionByStationUsingMultipleOutputs extends Configured implements Tool {

	
	static class StationMapper extends Mapper<LongWritable, Text, Text, Text>{
		private NcdcRecordParser parser = new NcdcRecordParser();	
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			parser.parse(value);
			context.write(new Text(parser.getStationId()), value);
		}		
	}

	static class MultipleOutputReducer extends Reducer<Text, Text, NullWritable, Text> {
		private NcdcRecordParser parser = new NcdcRecordParser();
		private MultipleOutputs<NullWritable, Text> multipleOutput;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutput = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> Values, Context context)
				throws IOException, InterruptedException {
			for(Text value : Values ){
				parser.parse(value);
				String basePath = String.format("%s/%s/part",parser.getStationId(), parser.getYear());
				
				multipleOutput.write(NullWritable.get(), value, basePath);
			}
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			multipleOutput.close();
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setMapperClass(StationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setReducerClass(MultipleOutputReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(), args);
		System.exit(exitCode);
	}
}