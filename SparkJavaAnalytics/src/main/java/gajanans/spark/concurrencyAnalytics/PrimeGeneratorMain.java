package gajanans.spark.concurrencyAnalytics;

public class PrimeGeneratorMain {

	public static void main(String[] args) {
		Thread task = new PrimeGenerator();
		task.start();

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		task.interrupt();

	}

}
