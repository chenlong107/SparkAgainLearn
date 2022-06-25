package home.chenlong.sparkstreaming_test.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Auther: chenlong
 * @Date: 2022/05/20/15:13
 * @Description:
 */
object transformOperator {

  def main(args: Array[String]): Unit = {
    // 一样要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

    val transformDStream: DStream[(String, Int)] = socketDStream.transform(
      rdd => {
        println("rdd的算子操作外 " + Thread.currentThread().getName)

        val value: RDD[(String, Int)] = rdd.flatMap(_.split(" "))
          .map {
            x =>
            println("rdd的算子操作里 " + Thread.currentThread().getName)
            (x, 1)
          }
          .reduceByKey(_ + _)

        value
      }
    )
    transformDStream.print()

    streamContext.start()

    streamContext.awaitTermination()

  }

}
