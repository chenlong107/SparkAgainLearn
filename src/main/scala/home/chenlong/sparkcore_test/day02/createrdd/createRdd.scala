package home.chenlong.sparkcore_test.day02.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: chenlong
 * @Date: 2022/05/01/14:18
 * @Description:
 */
object createRdd {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //集合方式一
//    val firstWay: RDD[Int] = sc.parallelize(Array(1,2,3,4),6)
//    firstWay.collect().foreach(println)

//    firstWay.saveAsTextFile("output666")
    //集合方式二，makeRDD其中一个底层是parallelize，另一个不是
//    val secondWay: RDD[Int] = sc.makeRDD(Array(1,2,3,4))
//    secondWay.collect().foreach(println)

    //由本地或hdfs文件创建：这里需要在resources下引入hdfs-site.xml，里面加入此属性(最好重启idea)，客户端使用本机ip作为节点ip，访问nn
    /*<property>
      <name>dfs.client.use.datanode.hostname</name>
      <value>true</value>
    </property>
    */
//    val hdfsFile: RDD[String] = sc.textFile("hdfs://hadoop101:9820/input_target")
    val hdfsFile: RDD[String] = sc.textFile("input/aaa.txt",3)
    hdfsFile.collect().foreach(println)


    sc.stop()
  }
}
