//package com.im.spark
//
///**
//  * Created by Administrator on 2016/4/19.
//  */
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.SparkContext._
//
///**
//  * 数据倾斜问题
//  */
//
//object SkewJoin {
//  def main(args: Array[String]) {
//    var conf = new SparkConf
//    conf.setAppName("first spark app!!!!")
//    conf.setMaster("local")
//    var spark = new SparkContext(conf)
//    val skewedTable = left.execute()
////    val spark = new SparkContext("local", "TopK", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
//    /*存在数据倾斜的数据表*/
//    val skewTable = spark.textFile("skewTable")
//    /*与skewTable连接的表*/
//    val Table = spark.textFile("Table")
//    /*对数据倾斜的表进行采样, 假设只有一个key倾斜最严重, 获取倾斜最大的key*/
//    val sample = skewTable.sample(false, 0.3, 9).groupByKey().collect()
//    val maxrowKey = sample.map(rows => (rows._2.size, rows._1))
//      .maxBy(rows => rows._1)._2)
//    /*将倾斜的表拆分为两个RDD, 一个为只含有倾斜key的表, 一个为不含有倾斜key的表*/
//    /*分别与原表连接*/
//    val maxKeySkewedTable = skewTable.filter(row => {
//      buildSideKeyGenerator(row) == maxrowKey
//    })
//    val mainSkewedTable = skewTable.filter(row => {
//      ！ (buildSideKeyGenerator(row) == maxrowKey)
//    })
//    /*分别与原表连接*/
//    val maxKeyJoinedRdd = maxKeySkewedTable.join(Table)
//    val mainJoinedRdd = mainSkewedTable.join(Table)
//    /*将结果合并*/
//    sc.union(maxKeyJoinedRdd, mainJoinedRdd)
//  }
//}
