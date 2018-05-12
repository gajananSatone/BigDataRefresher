package gajanans.HadoopPOC;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class HadoopUTIL implements Serializable{
	
	public static String getDirTimeSfix(){
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm");
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String dateSfix = sdf.format(timestamp);
		return dateSfix;
	}
}
