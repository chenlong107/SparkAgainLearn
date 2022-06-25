package home.chenlong.sparkcore_test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection

/**
 * @Auther: chenlong
 * @Date: 2022/05/13/15:54
 * @Description:
 */
object Action_Operator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Action_Operator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)
    val numRdd3: RDD[Int] = sc.makeRDD(Array(4,2,3,1,6,5),3)
    val numRdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a",1),("b",1),("b",2),("c",1),("c",3)),3)

//    val reduceRdd: Int = numRdd.reduce(_+_)
//    println(reduceRdd)

//    val collectRdd: Array[Int] = numRdd.collect()
//    collectRdd.foreach(println)

//    val countRdd: Long = numRdd.count()
//    println(countRdd)


//    val firstRdd: Int = numRdd.first()
//    println(firstRdd)

//    val takeRdd: Array[Int] = numRdd.take(3)
//    println(takeRdd.mkString(","))

//    val takeOrderedRdd: Array[Int] = numRdd.takeOrdered(3)
//    println(takeOrderedRdd.mkString(","))
//    numRdd3.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)

//    val aggregateRdd: Int = numRdd.aggregate(1)(math.max(_,_),_+_)
//    println(aggregateRdd)


//    val foldRdd: Int = numRdd.fold(1)(_+_)
//    println(foldRdd)

//    val countByKeyRdd: collection.Map[String, Long] = numRdd2.countByKey()
//    println(countByKeyRdd)

//    val saveTestRdd: Unit = numRdd.saveAsTextFile("TextOut")
//    println(saveTestRdd)


//    val saveObjectRdd: Unit = numRdd.saveAsObjectFile("ObjectOut")
//    println(saveObjectRdd)


//    val saveSeqRdd: Unit = numRdd2.saveAsSequenceFile("SequenceOut")
//    println(saveSeqRdd)

    numRdd3.collect().foreach(println)

    numRdd.foreach(println)

    //    Thread.sleep(1000000)

  }
}
