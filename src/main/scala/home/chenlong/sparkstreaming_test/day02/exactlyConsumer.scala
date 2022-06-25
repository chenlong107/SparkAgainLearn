package home.chenlong.sparkstreaming_test.day02

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.mutable



/**
 * @Auther: chenlong
 * @Date: 2022/05/21/14:49
 * @Description:
 */
object exactlyConsumer {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark_stream_wordCount").setMaster("local[*]")

//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val streamContext: StreamingContext = new StreamingContext(conf,Seconds(3))

    val kafkaParams = Map(
    "bootstrap.servers" -> "hadoop101:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer"-> classOf[StringDeserializer],
    "group.id" -> "3333",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)  // 手动维护，禁止自动提交，但问题是，kafka就不再记录current_offset了，但还记录leo
    )

// 这里是从一开始就将自动提交关闭的情况
//    GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                        HOST            CLIENT-ID
//    cl              second_topic    0          -               1               -               consumer-cl-1-f44d9752-e33d-41cd-80ea-e94df2b49561 /114.91.7.142   consumer-cl-1
//    cl              second_topic    1          -               3               -               consumer-cl-1-f44d9752-e33d-41cd-80ea-e94df2b49561 /114.91.7.142   consumer-cl-1

// 以分区0为例，这里是在offset=30的时候，将自动提交禁用，后面不在更新此CURRENT-OFFSET，但仍然记录LOG-END-OFFSET
//    GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
//    streamid        first_topic     0          30              38              8               -               -               -
//    streamid        first_topic     1          34              39              5               -               -               -


    val properties = new Properties()

    properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop101:3306/chenlong")
    properties.setProperty("username", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("maxActive", "3")

    val con: Connection = DruidDataSourceFactory.createDataSource(properties).getConnection

    val pre: PreparedStatement = con.prepareStatement("select * from chenlong.`offset_manager` where groupid = ?")

    pre.setObject(1,"3333")

    val set: ResultSet = pre.executeQuery()


    val offMap = mutable.Map[TopicPartition,Long]()

    while (set.next()){
      val top: TopicPartition = new TopicPartition(set.getString(2),set.getInt(3))
      val offset: Int = set.getInt(4)
      offMap.put(top,offset)  // 最后会把所有分区对应的offset放到一起，Map(first_topic-0 -> 38, first_topic-1 -> 39)
    }


    // 先看mysql里有没有这个组id，有就把它对应分区的offset取出来，
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = if(offMap.isEmpty){
      KafkaUtils.createDirectStream(
        streamContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("third_topic"), kafkaParams) // 不给offset那就按auto.offset.reset来读
      )
    } else{
      KafkaUtils.createDirectStream(
        streamContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("third_topic"), kafkaParams,offMap) // 如果给了offset，那就按这个offset来，如果下面不更新offset，那么这里一直都是按上次提交的offset来开始读
      )
    }

    // 这里要先取出消息里的value
    val wordNum: DStream[(String, Int)] = kafkaDStream.map(_.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wordNum.reduceByKeyAndWindow((a:Int,b:Int) => a+b,Seconds(6),Seconds(3)).print()

    kafkaDStream.foreachRDD(
      rdd => {
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val pre: PreparedStatement = con.prepareStatement("replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)")
        ranges.foreach(

          elem => {
            println(elem.topic,elem.partition,elem.untilOffset)
            pre.setObject(1,"3333")
            pre.setObject(2,elem.topic)
            pre.setObject(3,elem.partition)
            pre.setObject(4,elem.untilOffset)

            pre.executeUpdate()
          }
        )
      }
    )

    streamContext.start()
    streamContext.awaitTermination()
  }
}
