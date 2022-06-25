package home.chenlong.sparksql_test.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset, Row, SparkSession}

/**
 * @Auther: chenlong
 * @Date: 2022/05/18/14:03
 * @Description:
 */
object createThreeAbstract {
  def main(args: Array[String]): Unit = {

    // SparkSession同样需要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("createThreeAbstract").setMaster("local[*]")
    // ****注意**** 如果同时存在SparkContext，SparkSession，那要先创建SparkContext
    val sc: SparkContext = new SparkContext(conf)
    // 创建sparksql的入口，SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 创建一个RDD
    val userRDD: RDD[User] = sc.textFile("input/ccc.txt")
      .map {
        line =>
          val fileds = line.split(",")
          User(fileds(0).toInt, fileds(1))  // 将字段封装进样例类中
      }

    // 创建DataFrame的几种方式
    // 1、从文件中读取创建DataFrame，读取有两种方式：a: read后面直接(.对应格式)出来; b: read后接format，里面写对应格式名称
    // 这些格式有 csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile
    val df: DataFrame = spark.read.json("input/bbb.json")
    //    df.show()
    val df2: DataFrame = spark.read.format("json").load("input/bbb.json")
    //    df2.show()



    // 2、由rdd转换
    // 官网对于rdd转df，ds需要导入隐式转换
    import spark.implicits._    // 这里的spark是上面创建的SparkSession对象名
    val rddToDF: DataFrame = userRDD.toDF()
    //    rddToDF.show()

    // 3、由hive表中获取，这里用mysql代替
    val df3: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/chenlong")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "my_user")
      .load()
    //    df3.show()

    // 4、由Dataset转换
    val rddDS: Dataset[User] = userRDD.toDS()
    val df4: DataFrame = rddDS.toDF()
    //    df4.show()


    // 创建DataFrame的几种方式
    // 官网对于rdd转df，ds需要导入隐式转换
    // import spark.implicits._
    // 1、由rdd转换
    val rddToDS: Dataset[User] = userRDD.toDS()
    //    rddToDS.show()

    //2、由DataFrame转换
    val dfToDS: Dataset[User] = df.as[User]
    //    dfToDS.show()

    // 3、由集合转换
    val userSeq: Seq[User] = Seq(User(20,"zhangshan"),User(30,"lisi"),User(40,"wangwu"))
    val userDS: Dataset[User] = userSeq.toDS()
    //    userDS.show()



    // 关于Dataset、DataFrame转到rdd
    // 都是  xxx.rdd
    val dfToRDD: RDD[Row] = df.rdd   // 这里的返回值类型是RDD[Row]
    //    dfToRDD.collect().foreach(println)

    val dsToRDD: RDD[User] = userDS.rdd
    dsToRDD.collect().foreach(println)



    sc.stop()
    // 如果有SparkContext，那就不需要关闭SparkSession了，因为SparkSession的stop调的就是SparkContext.stop
    // spark.stop()

  }
}

case class User(age:BigInt,name:String){

}