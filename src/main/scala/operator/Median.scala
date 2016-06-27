package operator

/**
  * Created by Administrator on 2016/4/19.
  */


object Median {
  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Spark Pi")
//    val spark = new SparkContext(conf)
//    val data = spark.textFile("data")
//    /*将数据逻辑划分为10个桶, 这里用户可以自行设置桶数量, 统计每个桶中落入的数据量*/
//    val mappeddata = data.map(num => {
//      (num/1000, num)
//    })
//    val count = mappeddata.reduceByKey((a, b) => {
//      a + b
//    }).collect()
//    /*根据总的数据量, 逐次根据桶序号由低到高依次累加, 判断中位数落在哪个
//    桶中, 并获取到中位数在桶中的偏移量*/
//    val sum_count = count.map(data => {
//      data._2
//    }).sum
//    var temp = 0
//    var index = 0
//    var mid = sum_count / 2;
//    for (i <- 0 to 10) {
//      temp = temp + count(i);
//      if (temp >= mid) {
//        index = i
//        break;
//      }
//    }
//    /*中位数在桶中的偏移量*/
//    val offset = temp - mid
//    /*获取到中位数所在桶中的偏移量为offset的数, 也就是中位数*/
//    val result = mappeddata.filter(num => num._1 == index).takeOrdered(offset)
//    println("Median is " + result(offset))
//    spark.stop()
  }
}
