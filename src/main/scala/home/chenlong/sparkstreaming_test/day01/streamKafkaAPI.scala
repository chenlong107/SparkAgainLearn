package home.chenlong.sparkstreaming_test.day01

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.parsing.json.JSON



/**
 * @Auther: chenlong
 * @Date: 2022/05/20/13:34
 * @Description:
 */
object streamKafkaAPI {

  def main(args: Array[String]): Unit = {

    // 一样要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val kafkaParams = mutable.Map[String,Object]()  // 这里泛型写[String,Any]会报错，要用Object，因为下面的ConsumerStrategies.Subscribe泛型使用的是java的String
    kafkaParams.put("bootstrap.servers","hadoop101:9092")
    kafkaParams.put("key.deserializer" , classOf[StringDeserializer])
    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
    kafkaParams.put("group.id" , "streamid")
    kafkaParams.put("auto.offset.reset" , "earliest")


    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("first_topic"), kafkaParams)
    )

    // 这里要先取出消息里的value
    val wordNum: DStream[(String, Int)] = kafkaDStream.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    wordNum.print()

    streamContext.start()

    streamContext.awaitTermination()


  }

}
