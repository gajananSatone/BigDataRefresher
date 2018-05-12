package gajanans.spark.concurrencyAnalytics;

public class ThreadFactoryMain {

	public static void main(String[] args) {
		MyThreadFactory factory = new MyThreadFactory("MyThreadFactory");
		ThreadFactoryTask task = new ThreadFactoryTask();
		Thread thread;
		System.out.printf("Starting the Threads\n");
		
		for (int i = 0; i < 10; i++) {
			thread=factory.newThread(task);
			thread.start();
		}
		System.out.printf("Factory Stats \n");
		System.out.printf("%s \n", factory.getStats());

	}

}
