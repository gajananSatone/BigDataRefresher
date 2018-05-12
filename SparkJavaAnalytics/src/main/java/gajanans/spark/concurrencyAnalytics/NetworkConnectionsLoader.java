package gajanans.spark.concurrencyAnalytics;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class NetworkConnectionsLoader implements Runnable {

	@Override
	public void run() {
		System.out.printf("Beginning Network Connection loading: %s\n",new Date());

		try{
			TimeUnit.SECONDS.sleep(6);			
		}catch(InterruptedException e){
			e.printStackTrace();
		}

		System.out.printf("Network Connection loading has finished:%s\n",new Date());

	}

}
