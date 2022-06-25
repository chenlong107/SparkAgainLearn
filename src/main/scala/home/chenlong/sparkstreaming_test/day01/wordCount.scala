package home.chenlong.sparkstreaming_test.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Auther: chenlong
 * @Date: 2022/05/20/11:48
 * @Description:
 */
object wordCount {
  def main(args: Array[String]): Unit = {
    // 一样要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

    val flatWord: DStream[String] = socketDStream.flatMap(_.split(" "))
    val mapWord: DStream[(String, Int)] = flatWord.map((_, 1))
    val reduceWord: DStream[(String, Int)] = mapWord.reduceByKey(_ + _)
    reduceWord.print()

    streamContext.start()

    streamContext.awaitTermination()

  }
}
