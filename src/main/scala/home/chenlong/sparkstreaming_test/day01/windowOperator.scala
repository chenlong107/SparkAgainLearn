package home.chenlong.sparkstreaming_test.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Auther: chenlong
 * @Date: 2022/05/20/15:32
 * @Description:
 */
object windowOperator {

  def main(args: Array[String]): Unit = {
    // 一样要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

    val wordDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val windowDStream: DStream[(String, Int)] = wordDStream.window(Seconds(6),Seconds(3))
//
//    // wordDStream只是定义RDD的样式，windowDStream只是声明了窗口，还需要声明窗口里的计算方式
//    val windowWordCount: DStream[(String, Int)] = windowDStream.reduceByKey(_+_)

//    windowWordCount.print()

    wordDStream.reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(6),Seconds(3)).print()

    streamContext.start()
    streamContext.awaitTermination()

  }

}
