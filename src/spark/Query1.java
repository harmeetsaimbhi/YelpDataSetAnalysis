// Query1: List the unique categories of business with their addresses located in “Palo Alto”.

package spark;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Query1 {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("yelpDataAnalysis");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");

    JavaRDD<String> data = sc.textFile("dataset/business.csv");
    
    	JavaPairRDD<String,String> output = data.filter(x->x.contains("Palo Alto"))
    	.mapToPair(
    		line -> {
    			Tuple2<String, String> record = null;
    			String[] lineArr = line.split("::");
    			//String city = "Palo Alto";
    			String business = lineArr[2];
    			String address = lineArr[1];
    			return new Tuple2<String, String>(address, business);
    			
    		});

    JavaPairRDD<String, String> topTenRdd = sc.parallelizePairs(output.collect());
    
    String output_dir = "output1";

	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); 
	topTenRdd.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
    
    sc.stop();
    sc.close();
    
  }

}