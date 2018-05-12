package gajanans.SparkJavaAnalytics;

import gajanans.SparkJavaAnalytics.env.HadoopDataSources;
import gajanans.SparkJavaAnalytics.env.SparkJavaUtil;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class JavaBeanRDD {

	public static void main(String[] args) {
		
		ArrayList<HappyPerson> peopleList = new ArrayList<HappyPerson>();
		peopleList.add(new HappyPerson("holden", "coffee"));
		peopleList.add(new HappyPerson("Gajanan", "coffee"));
		peopleList.add(new HappyPerson("Saloni", "Dahi"));
		peopleList.add(new HappyPerson("Khushi", "Pizza"));
		
		SparkJavaUtil myUtil = new SparkJavaUtil();
		SparkSession jspark = myUtil.createJavaSparkSession("Java Bean RDD");
		
		JavaSparkContext jsc = new JavaSparkContext(jspark.sparkContext());
		
		JavaRDD<HappyPerson> happyPeopleRDD = jsc.parallelize(peopleList);
		
		Dataset<Row> df = jspark.createDataFrame(happyPeopleRDD, HappyPerson.class);
		df.createOrReplaceTempView("happyPeople");
		
		df.schema();
		df.show();
		
	}

}
