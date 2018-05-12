package gajanans.spark.concurrencyAnalytics;

import java.util.concurrent.TimeUnit;

public class FileSearchMain {

	public static void main(String[] args) {
		FileSearch searcher = new FileSearch("/home/khushi/", "spam.data");
		Thread thread = new Thread(searcher);
		thread.start();

		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		thread.interrupt();
	}
}
