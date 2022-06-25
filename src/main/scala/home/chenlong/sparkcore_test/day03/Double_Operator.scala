package home.chenlong.sparkcore_test.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: chenlong
 * @Date: 2022/05/12/20:43
 * @Description:
 */
object Double_Operator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Double_Operator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val numRdd: RDD[Int] = sc.makeRDD(Array(1,2,3),3)
    val numRdd2: RDD[Int] = sc.makeRDD(3 to 6,3)
    val numRdd3: RDD[String] = sc.makeRDD(List("a","b","c"),3)



//    val intersectionRdd: RDD[Int] = numRdd.intersection(numRdd2)
//    intersectionRdd.collect().foreach(println)

    val unionRdd: RDD[Int] = numRdd.union(numRdd2)
    unionRdd.collect().foreach(println)

    val substractRdd: RDD[Int] = numRdd.subtract(numRdd2)
    substractRdd.collect().foreach(println)

    val zipRdd: RDD[(Int, String)] = numRdd.zip(numRdd3)
    zipRdd.collect().foreach(println)


//        val zipRdd: RDD[(Int, Int)] = numRdd.zip(numRdd2)
//        zipRdd.collect().foreach(println)
  }
}
