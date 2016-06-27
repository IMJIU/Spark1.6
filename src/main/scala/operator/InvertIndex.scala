package operator

/**
  * Created by Administrator on 2016/4/19.
  */

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable._

object InvertIndex {
  def main(args: Array[String]) {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var spark = new SparkContext(conf)
    //    val spark = new SparkContext("local", "TopK", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    /*读取数据, 数据格式为一个大的HDFS文件中用\n分隔不同的文件, 用\t分隔文件ID和文件
    内容, 用" "分隔文件内的词汇*/
    val words = spark.textFile("dir").map(file =>
      file.split("\t")).map(item => {
      (item(0), item(1))
    }).flatMap(file => {
      /*将数据项转换为LinkedList[( 词, 文档id) ]的数据项, 并通过flatmap将RDD内
      的数据项转换为( 词, 文档ID) */
      val list = new LinkedList[(String, String)]
      val words = file._2.split(" ").iterator
      while (words.hasNext) {
        list + words.next()
      }
      list
    }).distinct()
    /*将( 词, 文档ID) 的数据进行聚集, 相同词对应的文档ID统计到一起,
    形成( 词, "文档ID1, 文档ID2 , 文档ID3……") , 形成简单的倒排索引*/
//    words.map(word => {
//      (word._2, word._1)
//    })
//      .reduce((a, b) => {
//        a + "\t" + b
//      })
//      .saveAsTextFile("index")
  }
}