package gajanans.SparkJavaAnalytics.env;

public class HadoopDataSources {
	public static String linkageFile = "hdfs://localhost:54310//gajanans/linkage";
	public static String userArtistData = "hdfs://localhost:54310/gajanans/last_fm/user_artist_data.txt";
	public static String artistData = "hdfs://localhost:54310/gajanans/last_fm/artist_data.txt";
	public static String artistAliasData = "hdfs://localhost:54310/gajanans/last_fm/artist_alias.txt";
	public static String apacheAccessLog = "hdfs://localhost:54310//gajanans/apacheLogs/access_log";
	public static String apacheErrorLog = "hdfs://localhost:54310//gajanans/apacheLogs/error_log";
	public static String spamData = "hdfs://localhost:54310//gajanans/spam.data";

	public static String results = "hdfs://localhost:54310//gajanans/results/";
	public static String lsLog = "hdfs://localhost:54310/gajanans/LearningSpark/log1.log";
	public static String longText = "hdfs://localhost:54310/gajanans/LearningSpark/LongText.txt";
	public static String numsLine = "hdfs://localhost:54310/gajanans/LearningSpark/Line_of_numbers.csv";
	public static String EmployeesNW = "hdfs://localhost:54310/gajanans/LearningSpark/NW-Employees-NoHdr.csv";

	public static String NameInfoJson = "hdfs://localhost:54310/gajanans/LearningSpark/pandainfo.json";
	public static String InfoSequenceFile = "hdfs://localhost:54310/gajanans/LearningSpark/pandainfo.seq";
	public static String SampleJason = "hdfs://localhost:54310/gajanans/LearningSpark/gjsSampleJason.json";
	
	public static String HamLogJason = "hdfs://localhost:54310/gajanans/LearningSpark/hamLog.json";
	
	public static String testweetJason = "hdfs://localhost:54310/gajanans/LearningSpark/testweet.json";

	public static String input = "hdfs://localhost:54310/gajanans/LearningSpark/input.txt";
}
