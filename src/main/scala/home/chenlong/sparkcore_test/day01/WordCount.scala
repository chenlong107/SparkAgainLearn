package home.chenlong.sparkcore_test.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: chenlong
 * @Date: 2022/04/29/19:32
 * @Description:
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val lineRdd: RDD[String] = sc.textFile("input")

    //所有RDD算子会在Executor端执行，此外的会在Driver端执行
    //flatMap、map、collect是RDD算子,reduceByKey是PairRDDFunctions,也是在executor端执行
    val wordRdd: RDD[String] = lineRdd.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = wordRdd.map((_,1))

    lineRdd.flatMap(_.split(" ")).groupBy(elem => elem).map{case (k,v) => (k,v.size)}.collect().foreach(println)


    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    wordToSum.cache()

//    println(wordToSum.toDebugString)
//
//    println("---------------------------------")
//
//    println(wordToSum.dependencies)
//
//    println("----------------------------------")

//    val wordToEnd: Array[(String, Int)] = wordToSum.collect()


//    println(wordToSum.toDebugString)
//
//    println("---------------------------------")
//    wordToEnd.foreach(println)

//    Thread.sleep(1000000)

    sc.stop()




  }
}
