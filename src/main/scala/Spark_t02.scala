import org.apache.spark._

import scala.io.Source

object Spark_t02 {
  val fileName: String = "D:\\access_log\\236\\localhost_access_log.2016-06-18.txt";
  val sougouFile: String = "D:\\log\\SogouQ2.reduced";
  val sougouSampleFile: String = "D:\\log\\SogouQ2.sample";
  def main(args: Array[String]) {
    sougou
//    userCount()
  }


  /**
    * 搜狗
    */
  def sougou(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(sougouFile, 1)
      .map(_.split('\t'))
      .filter(_.length == 6)
     // rdd.filter(a=>{println(a(3));a(3).toInt==1})
      .filter(_(3).toInt == 1)
      .filter(_(4).toInt ==1)
      .count
    println(rdd)
  }
  /**
    * 搜狗
    */
  def userCount(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.textFile(sougouSampleFile, 1)
      .map(_.split('\t'))
      .map(x=>(x(1),1))
      .reduceByKey(_+_)
      .map(x=>(x._2,x._1))
      .sortByKey(false)
      .map(x=>(x._2,x._1))
      .foreach(println)
  }
}
