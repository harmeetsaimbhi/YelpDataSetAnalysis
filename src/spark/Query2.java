/*Find the top ten rated businesses using the average ratings. Top
rated business will come first.  */

package spark;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Query2 {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("yelpDataAnalysis");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.setLogLevel("ERROR");

    JavaRDD<String> data = sc.textFile("dataset/review.csv");
    
    	JavaPairRDD<Double,String> output = data.mapToPair(
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
		.sortByKey(false);

    JavaPairRDD<Double, String> topTenRdd = sc.parallelizePairs(output.take(10));
    
    String output_dir = "output2";

	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true);
	topTenRdd.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
    
    sc.stop();
    sc.close();
    
  }

}