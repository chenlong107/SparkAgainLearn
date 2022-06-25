package home.chenlong.sparkstreaming_test.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Auther: chenlong
 * @Date: 2022/05/21/15:20
 * @Description:
 */
object test01 {

    def main(args: Array[String]): Unit = {

      val orderId = "666"

      val infoRedisKey = s"OrderInfo:$orderId"

      println(infoRedisKey)
    }
}
