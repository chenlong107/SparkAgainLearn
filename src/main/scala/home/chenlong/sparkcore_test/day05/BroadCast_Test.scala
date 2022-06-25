package home.chenlong.sparkcore_test.day05

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: chenlong
 * @Date: 2022/05/17/17:20
 * @Description:
 */
object BroadCast_Test {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("Accumulator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val wordRdd: RDD[String] = sc.makeRDD(List("hello","hive","hadoop","hbase"))

    // diver端的常量
    val str = "e"

    // 声明广播变量，将需要发送的值传入
    val strBroad: Broadcast[String] = sc.broadcast(str)

    // 通过调用 strBroad.value 获取这个广播值
    wordRdd.filter(_.contains(strBroad.value)).collect().foreach(println)

  }
}
