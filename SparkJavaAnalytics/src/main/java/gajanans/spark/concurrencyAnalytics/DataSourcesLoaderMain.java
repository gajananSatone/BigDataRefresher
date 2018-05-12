package gajanans.spark.concurrencyAnalytics;

import java.util.Date;

public class DataSourcesLoaderMain {

	public static void main(String[] args) {
		DataSourcesLoader dsLoader = new DataSourcesLoader();
		Thread thread1 = new Thread(dsLoader , "DataSourceThread");

		NetworkConnectionsLoader ncLoader = new NetworkConnectionsLoader();
		Thread thread2 = new Thread(ncLoader, "NetworkconnectionLoader");

		thread1.start();
		thread2.start();

		try{
			thread1.join();
			thread2.join();
		} catch (InterruptedException e){
			e.printStackTrace();
		}
		
		System.out.printf("Main: Configuration has been loaded:	%s\n",new Date());
	}
}
