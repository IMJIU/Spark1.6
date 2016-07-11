package streaming

import org.apache.spark._
import org.apache.spark.streaming._

/**
  * Created by Administrator on 2016/4/19.
  */
object T05_Window {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    //每5秒钟计算前10秒的数据
//    val wordsCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _,  Seconds(10), Seconds(5))
    val pairs = words.map(word => (word, 1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))

    //    val wordsCounts = words.map(x=>(x,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(15),Seconds(15))
    //    val sortedwc = wordsCounts.map{case (char,count)=>(count,char)}.transform(_.sortByKey(false)).map{case(char,count)=>(count,char)}
    windowedWordCounts.print()
    ssc.start() // Start the computation
    //    sortedwc.print();
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}