package operator

/**
  * Created by Administrator on 2016/4/19.
  */

import org.apache.spark.{SparkConf, SparkContext}


object NumOnce {
  def computeOneNum(args: Array[String]) {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var spark = new SparkContext(conf)
    //    val spark = new SparkContext("local[1]", "NumOnce", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val data = spark.textFile("data")
    /*每个分区分别对数据进行异或运算, 最后在reduceByKey阶段, 将各分区异或运算的结果再
    做异或运算合并。 偶数次出现的数字, 异或运算之后为0, 奇数次出现的数字, 异或后为数字本身*/
    //    val result = data.mapPartitions(iter => {
    //      var temp = iter.next()
    //      while (iter.hasNext) {
    //        temp = temp^iter.next()
    //      }
    //      Seq((1, temp)).iterator
    //    }).reduceByKey(_^_).collect()
    //    println("num appear once is : " + result)
  }
}
