package home.chenlong.sparkcore_test.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Auther: chenlong
 * @Date: 2022/05/17/14:23
 * @Description:
 */
object Accumulator_Test {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Accumulator_Test").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)


//    val wordRdd: RDD[String] = sc.makeRDD(List("Hello","Hello","Hello","Hive","Hbase"))
    val wordRdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",1)))

//    val reduceRdd: RDD[(String, Int)] = wordRdd.reduceByKey(_+_)

    val acc: LongAccumulator = sc.longAccumulator("str_acc")
    val accRdd: Unit = wordRdd.foreach {
      case (k, v) => acc.add(v)
    }

//    val mapRdd: RDD[(String, Int)] = wordRdd.map {
//      case (k, v) => {
//        acc.add(v)
//        (k, v)
//      }
//    }
//    mapRdd.collect()

//    println(acc.value)


//    val acc: MyAccumulate = new MyAccumulate()
//
//    sc.register(acc,"My_Acc")
//
//    val accRdd: Unit = wordRdd.foreach {
//       v => acc.add(v)
//    }

    println(acc.value)

    Thread.sleep(1100000)

    sc.stop()

  }
}


class MyAccumulate extends AccumulatorV2[String,mutable.Map[String,Int]] {

  // 提前创建最后的返回值
  var map = mutable.Map[String, Int]()

  // 判断此累加器是否为0,返回是boolean,重置累加器会调用此方法，按此方法判断累加器是否为空,list累加器是Nil,其他是0
  override def isZero: Boolean = map.isEmpty

  // 复制一个此累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAccumulate()

  // 重置累加器
  override def reset(): Unit = map.clear()

  //获取输入并累积
  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v,0) + 1
  }

  //将另一个相同类型的累加器合并到此累加器中，并更新其状态,即合并两个累加器，这里是合并两个map集合
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    // 合并两个集合，就要遍历其中一个集合，按另一个集合的键来找自己的值，再跟另一个集合的值相加
    other.value.foreach{
      case (k,v) => {
        map(k) = map.getOrElse(k,0) + v
      }
    }
  }

  // 返回当前累加器的结果
  override def value: mutable.Map[String, Int] = map

}
