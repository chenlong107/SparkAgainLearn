package home.chenlong.sparkstreaming_test.day01

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.util.Progressable
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @Auther: chenlong
 * @Date: 2022/05/20/16:32
 * @Description:
 */
object gracefullyStop {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    // 这里要开启优雅关闭，默认是false
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

    val wordDStream: DStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordDStream.print()

    new Thread(new MyStop(streamContext)).start()

    streamContext.start()
    streamContext.awaitTermination()

  }
}

// 优雅关闭原理就是，driver启动一个线程，来监控一个信号，这里是判断文件是否存在。如果有，那就关闭
class MyStop(ssc:StreamingContext) extends Runnable{
  override def run(): Unit = {
    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop101:9820"),new Configuration(),"hadoop")

    val stopPath: Path = new Path("hdfs://hadoop101:9820/stopStream")

    while (true){
      Thread.sleep(5000)
      if (fs.exists(stopPath)) {
        // 以下都是 StreamingContext自己的方法，包括stop
        val state: StreamingContextState = ssc.getState()

        if(state == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
          System.exit(0)
        }
      }
    }
  }
}

