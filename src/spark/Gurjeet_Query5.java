package spark;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Gurjeet_Query5 {

	public static void main(String[] args) throws Exception {
		long timeElapsed = System.currentTimeMillis();
		System.out.println("Started Processing");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("YouTubeDM");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE,
		// WARN
		sc.setLogLevel("ERROR");

		JavaRDD<String> review = sc.textFile("dataset/review.csv");

		JavaPairRDD<String, Integer> reviewData = review.mapToPair(line -> {
			String[] lineArr = line.split("::");
			String userID = lineArr[0];
			//String stars = lineArr[3];

			return new Tuple2<String, Integer>(userID, 1);
		}).reduceByKey((x,y)->x+y);
 
	//	Map<String, Long> test = reviewData.countByKey();
		System.out.println(reviewData.count());
		//System.out.println("hello");
	//	JavaPairRDD<String, String> topTenRdd = sc.parallelizePairs(finalData.take(100));

		 String output_dir = "output";
		    
			//remove output directory if already there
			FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
			fs.delete(new Path(output_dir), true); // delete dir, true for recursive
			reviewData.saveAsTextFile(output_dir);
			timeElapsed = System.currentTimeMillis() - timeElapsed;
			System.out.println("Done.Time taken (in seconds): " + timeElapsed/1000f);   
		sc.stop();
		sc.close();

	}

}