/* List the  business_id , full address and categories of the Top 10
businesses using the average ratings. (Reduce side join and job chaining technique)
Display all the information of each business from business.csv */

package spark;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Query3 {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("yelpDataAnalysis");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");

    JavaRDD<String> reviewContent = sc.textFile("dataset/review.csv");
    JavaRDD<String> businessContent = sc.textFile("dataset/business.csv");
    
    	List<Tuple2<Double,String>> topTenReviewMap = reviewContent.mapToPair(
    		line -> {
    			String[] lineArr = line.split("::");
    			String businessID = lineArr[2];
    			Double rating = Double.parseDouble(lineArr[3]);
    			Tuple2<Double, Integer> ratingTuple = new Tuple2<Double, Integer>(rating,1);
    			return new Tuple2<String, Tuple2<Double, Integer>>(businessID, ratingTuple);
    		})
    	.reduceByKey((x, y) -> new Tuple2<Double, Integer>(x._1 + y._1, x._2 + y._2))
    	.mapToPair(x -> new Tuple2<String,Double>(x._1,(x._2._1/x._2._2)))
		.mapToPair(item -> item.swap())
		.sortByKey(false).take(10);
    	
    	JavaPairRDD<String, Double> topTenRdd = sc.parallelizePairs(topTenReviewMap).mapToPair(item -> item.swap());
    	
    	JavaPairRDD<String, Tuple2<String, String>> businessMap = businessContent.mapToPair(
        		line -> {
        			String[] lineArr = line.split("::");
        			String businessID = lineArr[0];
        			String address = lineArr[1];
        			String category = lineArr[2];
        			Tuple2<String, String> businessTuple = new Tuple2<String, String>(address,category);
        			return new Tuple2<String, Tuple2<String, String>>(businessID, businessTuple);
        		}).sortByKey(false);
        	
    	JavaPairRDD<String, Tuple2<Double, Tuple2<String, String>>> joined = topTenRdd.join(businessMap).distinct();
    
    String output_dir = "output3";
    
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true);
	joined.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
    
    sc.stop();
    sc.close();
    
  }

}