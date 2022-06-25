package home.chenlong.sparkstreaming_test.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Auther: chenlong
 * @Date: 2022/05/20/16:15
 * @Description:
 */
object outputTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

    val wordDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    // 流的foreachRDD，rdd的foreachPartition
    wordDStream.foreachRDD(
      rdd => rdd.foreachPartition(elem => elem.foreach(println))
    )

    streamContext.start()

    streamContext.awaitTermination()

  }

}
