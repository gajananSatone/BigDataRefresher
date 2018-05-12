//package gajanans.HadoopPOC;
//
//import org.apache.hadoop.conf.Configuration;
//import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.Matchers.greaterThan;
//import static org.junit.Assert.assertThat;
//
//public class ReadConfiguration {
//
//	public static void main(String[] args) {
//		Configuration conf = new Configuration();
//		conf.addResource("configuration-1.xml");
//		assertThat(conf.get("color"), is("yellow"));
//		assertThat(conf.getInt("size", 0), is(10));
//		assertThat(conf.get("breadth", "wide"), is("wide"));
//
//	}
//
//}
