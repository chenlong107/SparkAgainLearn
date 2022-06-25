package home.chenlong.sparkcore_test.day03

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Auther: chenlong
 * @Date: 2022/05/13/15:52
 * @Description:
 */
object KeyValue_Operator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KeyValue_Operator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[(String,Int)] = sc.makeRDD(Array(("a",1),("b",1),("b",2),("c",1),("c",3)),3)
    val numRdd2: RDD[(String,Double)] = sc.makeRDD(Array(("a",11.0),("a",99.0),("b",11.0),("b",12.0),("d",99.0)))

    //    val partitionByRdd: RDD[(Int, String)] = numRdd.partitionBy(new HashPartitioner(2))
    //    val partitionByRdd: RDD[(Int, String)] = numRdd.partitionBy(new MyPartitioner(2))
    //    partitionByRdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)

    //    val reduceByKeyRdd: RDD[(String,Int)] = numRdd.reduceByKey(_ + _)
    //    reduceByKeyRdd.collect().foreach(println)

    //    val distinctRdd: RDD[Int] = numRdd2.distinct()
    //    distinctRdd.collect().foreach(println)


    //    val map2: RDD[Int] = reduce1.map(_._1)
    //    map2.collect().foreach(println)
    //    reduce1.collect().foreach(println)
    //    map1.collect().foreach(println)

    //    val groupByKeyRdd: RDD[(String, Iterable[Int])] = numRdd.groupByKey(3)
    //    groupByKeyRdd.collect().foreach(println)

    //    numRdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)

    val aggregateByKeyRdd: RDD[(String, Int)] = numRdd.aggregateByKey(0)(math.max(_,_),_+_)
    aggregateByKeyRdd.collect().foreach(println)



    //    val foldByKeyRdd: RDD[(String, Int)] = numRdd.foldByKey(0)(_+_)
    //    foldByKeyRdd.collect().foreach(println)

//        val combineByKeyRdd: RDD[(String, (Int, Int))] = numRdd.combineByKey(
    //      x => (x,1),
    //      (acc,v) => (acc._1+v,acc._2+1),
    //      (acc1,acc2:(Int,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2)
    //    )
    //    combineByKeyRdd.map{case (k,v) => (k,v._1 / v._2.toDouble)}.collect().foreach(println)

    //    val sortByKeyRdd: RDD[(String, Int)] = numRdd.sortByKey(true,3)
    //    sortByKeyRdd.collect().foreach(println)

    //    val mapValuesRdd: RDD[(String, Int)] = numRdd.mapValues(_ * 2)
    //    mapValuesRdd.collect().foreach(println)

    //    val joinRdd: RDD[(String, (Int, Double))] = numRdd.join(numRdd2)
    //    joinRdd.collect().foreach(println)

//    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Double]))] = numRdd.cogroup(numRdd2)
//    cogroupRdd.collect().foreach(println)

    //    (a,(CompactBuffer(1),CompactBuffer(11.0, 99.0)))
    //    (b,(CompactBuffer(1, 2),CompactBuffer(11.0, 12.0)))
    //    (c,(CompactBuffer(1, 3),CompactBuffer()))
    //    (d,(CompactBuffer(),CompactBuffer(99.0)))

  }
}

class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    if(key.isInstanceOf[Int]){
      val keyInt: Int = key.asInstanceOf[Int]
      if(keyInt % 2 == 0){
        1
      } else{
        0
      }
    }else{
      0
    }
  }
}
