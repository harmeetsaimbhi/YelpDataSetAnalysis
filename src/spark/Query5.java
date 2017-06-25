package spark;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Query5 {
	
  public static void main(String[] args) throws Exception {
    long timeElapsed = System.currentTimeMillis();
    System.out.println("Started Processing");
    SparkConf conf = new SparkConf()
    .setMaster("local")
    .setAppName("YouTubeDM");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("ERROR");

    JavaRDD<String> reviewContent = sc.textFile("dataset/review.csv");
    JavaRDD<String> userContent = sc.textFile("dataset/user.csv");

    
    //JavaRDD<String> sortedData = data.collect() //directory where the files are
    
    	JavaPairRDD<Integer, String> topBusinesses = reviewContent.mapToPair(
    		line -> {
    			String[] lineArr = line.split("::");
    			String userID = lineArr[1];
    			return new Tuple2<String, Integer>(userID, 1);
    		})
    	.reduceByKey((x, y) ->(x + y))
		.mapToPair(item -> item.swap());


    JavaPairRDD<String, Integer> topTenBusiness  = sc.parallelizePairs(topBusinesses.sortByKey(false).collect()).mapToPair(item -> item.swap());
    	
  
    JavaPairRDD<String, String> userMap = userContent.mapToPair(
    		line -> {
    			String[] lineArr = line.split("::");
    			String userID = lineArr[0];
    			String userName = lineArr[1];
    			//String category = lineArr[2];
    			Tuple2<String, String> businessTuple = new Tuple2<String, String>(userID,userName);
    			return new Tuple2<String,String>(userID, userName);
    		});
		//.mapToPair(item -> item.swap())
		//.sortByKey(false);
    
   // JavaPairRDD<String, Tuple2<Integer, String>> joined = topTenBusiness.join(userMap).sortByKey(false); 
    
    String output_dir = "output5";
    
	//remove output directory if already there
	FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
	fs.delete(new Path(output_dir), true); // delete dir, true for recursive
	topTenBusiness.saveAsTextFile(output_dir);
	timeElapsed = System.currentTimeMillis() - timeElapsed;
	System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);
//	System.out.println("the count is:" + count);
	//System.out.println("Processed Records: " + count);
    
    sc.stop();
    sc.close();
    
  }

}