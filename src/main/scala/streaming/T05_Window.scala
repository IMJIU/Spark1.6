package streaming

import org.apache.spark._
import org.apache.spark.streaming._

/**
  * Created by Administrator on 2016/4/19.
  */
object T05_Window {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    //每10秒钟计算前15秒的数据
    val wordsCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(10), Seconds(5))

    //    val wordsCounts = words.map(x=>(x,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(15),Seconds(15))
    //    val sortedwc = wordsCounts.map{case (char,count)=>(count,char)}.transform(_.sortByKey(false)).map{case(char,count)=>(count,char)}
    wordsCounts.print()
    //    sortedwc.print();
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}