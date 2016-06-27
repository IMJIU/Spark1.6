package streaming

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

/**
  * Created by Administrator on 2016/4/19.
  */
object T03_NetworkWordCount {

    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(1))
      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines = ssc.socketTextStream("localhost", 9999)
      // Split each line into words
      val words = lines.flatMap(_.split(" "))
      import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
      // Count each word in each batch
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      // Print the first ten elements of each RDD generated in this DStream to the console
      wordCounts.print()
      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
    }

}