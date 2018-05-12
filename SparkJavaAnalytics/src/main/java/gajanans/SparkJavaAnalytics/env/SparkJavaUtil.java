package gajanans.SparkJavaAnalytics.env;
import java.awt.image.ImagingOpException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkConf;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkJavaUtil implements Serializable{

	private static final long serialVersionUID = 1L;
	private String sparkMaster;
	private String propertiesFileName;

	public SparkJavaUtil (){
		this.propertiesFileName = "SparkJavaClusterDefault.properties";
		this.setSparkEnv();
	}

	public SparkJavaUtil (String propertiesFileName){
		this.propertiesFileName = propertiesFileName;
		this.setSparkEnv();
	}

	public Double toDouble(String s){
		if (s.equals("?")){

			return Double.NaN;
		} else
		{
			return Double.parseDouble(s);
		}
	}

	public SparkSession createSparkSession (String appName){

		SparkConf conf = new SparkConf()
		.setAppName(appName)
		.setMaster(this.sparkMaster);		

		SparkSession spark = SparkSession
				.builder()
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		return spark;		

	}
	
	public JavaStreamingContext createJavaSparkStreamContext(String appName , Integer duration){
		
		SparkConf conf = new SparkConf()
		.setAppName(appName)
		.setMaster(this.sparkMaster);
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(duration));
		return jsc;
	}
	
	public JavaSparkContext createSparkContext (String appName){

		SparkConf conf = new SparkConf()
		.setAppName(appName)
		.setMaster(this.sparkMaster);
		

		System.out.println("Env set to use SPARK MASTER as -  " + this.sparkMaster);

		JavaSparkContext sc = new JavaSparkContext(conf);

//		SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		return sc;		

	}
	
	public SparkSession createJavaSparkSession (String appName){

		SparkConf conf = new SparkConf()
		.setAppName(appName)
		.setMaster(this.sparkMaster);
		

		System.out.println("Env set to use SPARK MASTER as -  " + this.sparkMaster);

//		JavaSparkContext sc = new JavaSparkContext(conf);

		SparkSession jSpark = SparkSession
				.builder()
				.config(conf)
				.enableHiveSupport()
				.getOrCreate();
		return jSpark;		

	}

	public Double naz (Double d) {
		if (d.isNaN()){
			return 0.0;
		}else {
			return d;
		} 
	}

	public String getSparkMaster() {
		return sparkMaster;
	}

	private void setSparkEnv(){
		setSparkMaster(this.propertiesFileName);
	}

	private void setSparkMaster(String propFileName) {

		@SuppressWarnings("unused")
		Properties prop = new Properties();

		try {
			File file = new File(propFileName);
			FileInputStream fileInput = new FileInputStream(file);
			Properties properties = new Properties();
			properties.load(fileInput);
			fileInput.close();
			this.sparkMaster = properties.getProperty("MASTER");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ImagingOpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	} 
	public String getDirTimeSfix(){
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm");
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String dateSfix = sdf.format(timestamp);
		return dateSfix;
	}

}
