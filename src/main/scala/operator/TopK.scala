package operator

/**
  * Created by Administrator on 2016/4/19.
  */

import java.util

import org.apache.spark.{SparkConf, SparkContext}

object TopK {
  val list = new util.LinkedList[Any]()
  val map = new util.LinkedHashMap[String,Int]()
  def main(args: Array[String]) {
    var path = "/tmp/stop_easyconnect.sh"
    /*执行WordCount, 统计出最高频的词*/
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var spark = new SparkContext(conf)
    //    val spark = new SparkContext("local", "TopK", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val count = spark.textFile(path)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    /*统计RDD每个分区内的Top K查询*/
    val topk = count.mapPartitions(iter => {
      while (iter.hasNext) {
        putToHeap(iter.next())
      }
      getHeap().iterator
    }).collect()

    /*将每个分区内统计出的TopK查询合并为一个新的集合, 统计出TopK查询*/
    val iter = topk.iterator
    while (iter.hasNext) {
      putToHeap(iter.next())
    }
    val outiter = getHeap().iterator
    /*输出TopK的值*/
    println("Topk 值 : ")
    while (outiter.hasNext) {
      println("\n 词频: " + outiter.next()._1 + " 词: " + outiter.next()._2)
    }
    spark.stop()
  }

  def putToHeap(iter: (String, Int)) {
    /*数据加入含k个元素的堆中*/
    //……
    map.put(iter._1,map.getOrDefault(iter._1,0)+1)
  }

  def getHeap(): Array[(String, Int)] = {
    /*获取含k个元素的堆中的元素*/
    val a = new Array[(String, Int)](2)
    //……
    a
  }
}

