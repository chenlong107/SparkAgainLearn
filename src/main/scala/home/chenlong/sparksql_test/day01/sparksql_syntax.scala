package home.chenlong.sparksql_test.day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Auther: chenlong
 * @Date: 2022/05/18/16:17
 * @Description:
 */
object sparksql_syntax {

  def main(args: Array[String]): Unit = {
    // SparkSession同样需要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("createThreeAbstract").setMaster("local[*]")
    // 创建sparksql的入口，SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 创建DataFrame
    val df: DataFrame = spark.read.json("input/bbb.json")

    // 1、sql风格(必须要有视图)：
    // 将DataFrame变成一个临时视图(仅对当前窗口有效)，相当于中间的虚拟表，之后可以用sql语法查这个视图名
    // 如果是创建全局视图(对所有窗口有效)，可以用 df.createOrReplaceGlobalTempView("user_temp")。 使用时要加上一个特定库名：global_temp
    df.createOrReplaceTempView("user_temp")

    val df2: DataFrame = spark.sql("select name ,age + 1 as new_age from user_temp")
    //    df2.show()

    val df3: DataFrame = spark.sql("select avg(age) from user_temp")
    //    df3.show()

    // 2、（domain-specific language，DSL）风格，不用创建视图，以本身DataFrame为中心，来操作数据

    val dsl1: DataFrame = df.select("age","name")
    //    dsl1.show()

    val dsl2: Dataset[Row] = df.select("*").where("age>19")
    //    dsl2.show()

    // 这里如果有一个字段需要计算，那么都要加上$符号，除非使用（一个单引号）修饰计算的字段；此外只要是计算都要加上隐式转换
    import spark.implicits._
//    df.select($"name",$"age" + 1).show
    df.select('name,'age + 1 as "new_age").show


    spark.stop()

  }

}
