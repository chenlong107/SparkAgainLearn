package home.chenlong.sparkcore_test.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: chenlong
 * @Date: 2022/05/14/16:28
 * @Description:
 */
object ScalaSerializable {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Scala_Serializable_Test").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化的自定义类
      .registerKryoClasses(Array(classOf[MySerializable]))
    val sc: SparkContext = new SparkContext(conf)

    val strRdd: RDD[String] = sc.makeRDD(Array("hello","hello word","hadoop","spark"))
    val serial: MySerializable = new MySerializable("hello")

    val matchRdd: RDD[String] = serial.getMatchOne(strRdd)
    matchRdd.foreach(println)

  }
}

class MySerializable(queryWord: String) extends Serializable{

  def getMatchOne(rdd:RDD[String])={
//    val s = queryWord
    rdd.filter(_.contains(queryWord))
  }

}
