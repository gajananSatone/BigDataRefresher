package gajanans.mrV4;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Log to stdout file
		System.out.println("Map key: " + key);
		// Log to syslog file
	
		LOG.info("Map key: " + key);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Map value: " + value);
		}
		context.write( key, value);
	}
}
