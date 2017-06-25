/* List the top ten businesses in terms of maximum number of reviews. */

package spark;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Query4 {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("yelpDataAnalysis");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");

    JavaRDD<String> reviewContent = sc.textFile("dataset/review.csv");
    JavaRDD<String> businessContent = sc.textFile("dataset/business.csv");

    	JavaPairRDD<Integer, String> topBusinesses = reviewContent.mapToPair(
    		line -> {
    			String[] lineArr = line.split("::");
    			String businessID = lineArr[2];
    			return new Tuple2<String, Integer>(businessID, 1);
    		})
    	.reduceByKey((x, y) ->(x + y))
		.mapToPair(item -> item.swap());


    JavaPairRDD<String, Integer> topTenBusiness = sc.parallelizePairs(topBusinesses.sortByKey(false).mapToPair(item -> item.swap()).take(10));
    
    String output_dir = "output4";

	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); 
	topTenBusiness.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
    
    sc.stop();
    sc.close();
    
  }

}