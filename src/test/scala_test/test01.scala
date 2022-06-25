import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import com.alibaba.fastjson.JSON
import scala.collection.mutable

/**
 * @Auther: chenlong
 * @Date: 2022/05/16/13:41
 * @Description:
 */
object test01 {
  def main(args: Array[String]): Unit = {

//    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
//    val sc: SparkContext = new SparkContext(conf)

//    sc.parallelize(1 to 4, 2).mapPartitionsWithIndex((index,ite) => ite.map((index,_))).collect().foreach(print)

//    val tuple: (Int, Int) = sc.parallelize(1 to 4, 2).aggregate(0,0)((x:(Int,Int),y:Int) => {print(x,y)
//      (x._1 + y,x._2 + 1)},(x:(Int,Int),y:(Int,Int)) => (x._1 + y._1,x._2 + y._2))
//    println(tuple)

//    val value: RDD[String] = sc.textFile("input/ddd.txt")
//    val value1: RDD[String] = value.map {
//      elem =>
//        val tmp_num: Int = (elem.toInt + 7770777) / 10
//        var f1 = (tmp_num / 1000000 % 10).toString
//        var f2 = (tmp_num / 100000 % 10).toString
//        var f3 = (tmp_num / 10000 % 10).toString
//        var f4 = (tmp_num / 1000 % 10).toString
//        var f5 = (tmp_num / 100 % 10).toString
//        var f6 = (tmp_num / 10 % 10).toString
//        var f7 = (tmp_num / 1 % 10).toString
//        val curr_num: String = f7 + f6 + f3 + "*" + f5 + f2 + f1
//        curr_num
//    }
//    value1.collect().foreach(print)

//    sc.stop()
// 一样要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(5))


    val kafkaDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("hadoop101",9999)

//    kafkaDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val data: DStream[(String, Int)] = kafkaDStream.map(record => {
      val data_source: Data_Source = JSON.parseObject(record, classOf[Data_Source])
      (data_source.owner, data_source.columnNum)
    })

    val data_window: DStream[(String, Int)] = data.window(Seconds(30))

    val result: DStream[(String, Int)] = data_window.reduceByKey(_+_)

    result.print()

    streamContext.start()

    streamContext.awaitTermination()

  }
}

case class Data_Source(
                        createdTime:String,
                        owner:String,
                        operationType:String,
                        columnNum:Int
                      )
