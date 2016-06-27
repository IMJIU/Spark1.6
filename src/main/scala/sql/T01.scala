package sql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/4/30.
  */
object T01 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    var sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._ //导⼊各种sql操作的⼊⼝与各种隐式转换
    val data = sc.textFile("/usr/people")
    case class Person(name:String,age:Int)
    val people = data.map(_.split(",")).map(p=>Person(p(0),p(1).toInt))
    val parfile = sqlContext.parquetFile()
  }
}
