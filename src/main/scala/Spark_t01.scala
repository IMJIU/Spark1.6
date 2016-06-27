import org.apache.spark._

import scala.io.Source
object Spark_t01 {

  def main(args: Array[String]) {

    //        wordCount();
//        exeCount_click_nopic()
    //    exeCount2_click_withpic()
    //    exeCount3_sum()
//    exeCount4_avg()
//    exeCount5_mix()
    //    println("123123ms")
//    hdfs_test();
//    t01()
//    t02()
//    exeCount4_avg();
//    join();
  }
  def t02(): Unit ={
    val v = Array("id", "logisticsId", "logisticsCompanyName", "expressState", "address", "customerName", "addressName", "addressPhoneNumber", "postCode", "customerPhoneNumber", "createTime", "payTime", "stateName", "tradeCode", "tradeType", "drugMoney", "buyNumber", "drugTypeName", "drugId", "drugName", "amount", "unit", "price")
    val lines = Source.fromFile("C:\\Users\\Administrator\\Desktop\\a.txt").getLines();
    v.foreach(s=>{
      var ok = false;
      lines.foreach(line=>{
        if (line.indexOf(s)>=0){
          ok = true;
        }
      });
      if (!ok)println("%s is not exists".format(s));
    })
  }
  def t01(): Unit ={
    val d = Array(("a",1),("b", 2),("a" ,3), ("b",4))
//    d.reduceLeft((a,b)=>{if (a._1==b._1)(a._1,a._1+b._2)else})
  }

  def hdfs_test(): Unit ={
    var conf = new SparkConf()
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    //    var lines = sc.textFile("d://log_network.txt",1)
    var lines = sc.textFile("hdfs://192.168.137.210:9000/input/a.log",1)
    var words = lines.flatMap{line=>line.split(" ")}

    var pairs = words.map{word => (word,1)}

    var wc = pairs.reduceByKey(_+_)

    wc.foreach(wnum=>println(wnum._1+":"+wnum._2))
  }

  def wordCount(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("d://log_network.txt", 1)
      .flatMap { line => line.split(" ") }.map { word => (word, 1) }.reduceByKey(_ + _).foreach(wnum => println(wnum._1 + ":" + wnum._2))
  }

  def exeCount_click_nopic(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("g://access_log.txt", 4)
      .filter(string => string.indexOf( """/api/""") != -1 && string.indexOf( """/api/open/file""") == -1)
      .map(line => line.substring(line.indexOf( """/api/""")))
      .map(line => (line.substring(0, line.indexOf(" ")),1))
      .reduceByKey(_+_).map(t=>(t._2,t._1))
      .sortByKey()
      .foreach(w=>println(w._1+":"+w._2))
  }

  def exeCount2_click_withpic(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("e://localhost_access_log.2016-04-07.txt", 4)
      .filter(string => string.indexOf( """/api/""") != -1 )
      .map(line => line.substring(line.indexOf( """/api/""")))
      .map(line => { if ( line.indexOf( """/api/open/file""") != -1) line.substring(0,line.indexOf("/",16))+" " else line})
      .map(line => (line.substring(0, line.indexOf(" ")),1))
      .reduceByKey(_+_).map(t=>(t._2,t._1))
      .sortByKey()
      .foreach(w=>println(w._1+":"+w._2))
  }
  def exeCount3_sum(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("e://info.log.2016-04-07", 4)
      .filter(string => string.indexOf( """@Cost""") != -1 )
      .map(line => {val v = line.split(" ") ; (v(6).substring(0,v(6).indexOf("@Cost")),v(7).substring(0,v(7).indexOf("ms")).toInt)})
      .reduceByKey(_+_)
      .map(t=>(t._2,t._1))
      .sortByKey()
      .foreach(w=>println(w._1/1000+"s : "+w._2))
    //groupByKey().map{ x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true))}.collect Array[(String, Int)] = Array((B,5), (C,3), (A,4))
  }

  /**
    * 平均数
    */
  def exeCount4_avg(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("g://info.log.2016-04-07", 4)
      .filter(string => string.indexOf( """@Cost""") != -1 )
      .map(line => {val v = line.split(" ") ; (v(6).substring(0,v(6).indexOf("@Cost")),v(7).substring(0,v(7).indexOf("ms")).toInt)})
      .groupByKey().map{ x=>(x._1, x._2.reduce(_+_)/x._2.count(x=>true))}.collect
      .sortBy(_._2)
      .foreach(w=>println(w._2+"ms : "+w._1))
    //
  }

  /**
    *
    */
  def exeCount5_mix(): Unit = {
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
    sc.textFile("e://info.log.2016-04-07", 4)
      .filter(string => string.indexOf( """@Cost""") != -1 )
      .map(line => {val v = line.split(" ") ; (v(6).substring(0,v(6).indexOf("@Cost")),v(7).substring(0,v(7).indexOf("ms")).toInt)})
      .groupByKey().map{ x=>(x._1,  x._2.reduce(_+_)/x._2.count(x=>true))}
      .sortBy(_._2)
      .foreach(w=>println(w._2+"ms : "+w._1))
    //
  }

  /**
    * join操作
    */
  def join(): Unit ={
    var conf = new SparkConf
    conf.setAppName("first spark app!!!!")
    conf.setMaster("local")
    var sc = new SparkContext(conf)
     val fcache =  sc.textFile("g://info.log.2016-04-07", 1).filter(string => string.indexOf( """@Cost""") != -1 ).cache
    var left = fcache.map(line => {val v = line.split(" ") ; (v(6).substring(0,v(6).indexOf("@Cost")),1)}).reduceByKey(_+_)
    val right = fcache.map(line => {val v = line.split(" ") ; (v(6).substring(0,v(6).indexOf("@Cost")),v(7).substring(0,v(7).indexOf("ms")).toInt)})
      .groupByKey().map{ x=>(x._1, (x._2.reduce(_+_)/x._2.count(x=>true))+"ms")}
      left.join(right).sortBy(_._2._1).foreach(w=>println(w._1+" 访问次数："+w._2._1+" 平均耗时:"+w._2._2))
  }
}
