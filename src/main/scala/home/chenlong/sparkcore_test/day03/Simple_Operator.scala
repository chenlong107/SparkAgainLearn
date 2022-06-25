package home.chenlong.sparkcore_test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
 * @Auther: chenlong
 * @Date: 2022/05/12/14:13
 * @Description:
 */
object Simple_Operator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Operator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(1 to 5,4)
//    val numRdd2: RDD[Int] = sc.makeRDD(5 to 10,3)

    val textRdd: RDD[String] = sc.textFile("input")

////    def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U],preservesPartitioning: Boolean = false)
//    val mappRdd: RDD[Int] = numRdd.mapPartitions(_.map(_ * 2))
//    mappRdd.collect().foreach(println)
//
////    def mapPartitionsWithIndex[U: ClassTag](f: (Int, Iterator[T]) => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    val mappIndex: RDD[(Int, Int)] = numRdd.mapPartitionsWithIndex((index,itor) => itor.map((index,_)))
    mappIndex.collect().foreach(println)

    val glomRdd: RDD[Int] = numRdd.glom().map(_.max)
    glomRdd.collect().foreach(println)

//    val groupByRdd: RDD[(Int, Iterable[Int])] = numRdd.groupBy(_ % 2)
//    groupByRdd.collect().foreach(println)

//    val gpWordCount: RDD[(String, Iterable[(String, Int)])] = textRdd.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)

//    def sample(withReplacement: Boolean, fraction: Double, seed: Long = Utils.random.nextLong): RDD[T]
//    val sampleRdd: RDD[Int] = numRdd.sample(true,1,10)
//    sampleRdd.collect().foreach(println)


//    def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
//    val distinctRdd: RDD[Int] = numRdd.distinct(3)
//    distinctRdd.collect().foreach(println)

//      val coaleaseRdd: RDD[Int] = numRdd.coalesce(2)
//      coaleaseRdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)

//    val repartitionRdd: RDD[Int] = numRdd.repartition(3)
//    repartitionRdd.mapPartitionsWithIndex((index,iter) => iter.map((index,_))).collect().foreach(println)

//    Thread.sleep(1000000)

//    val sortByRdd: RDD[Int] = numRdd.sortBy(elem => elem,false,3)
//    sortByRdd.collect().foreach(println)

//    val pipRdd: RDD[String] = numRdd.pipe("touch aaa.txt")
//    pipRdd.collect().foreach(println)

//    val intersectionRdd: RDD[Int] = numRdd.intersection(numRdd2)
//    intersectionRdd.collect().foreach(println)


  }
}
