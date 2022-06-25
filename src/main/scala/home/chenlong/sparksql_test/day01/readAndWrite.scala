package home.chenlong.sparksql_test.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Auther: chenlong
 * @Date: 2022/05/18/17:10
 * @Description:
 */
object readAndWrite {

  def main(args: Array[String]): Unit = {
    // SparkSession同样需要创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("createThreeAbstract").setMaster("local[*]")
//    conf.set("spark.sql.sources.default","json")
    // 创建sparksql的入口，SparkSession
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 读取
    // 读取有两种方式：a: read后面直接(.对应格式)出来; b: read后接format，里面写对应格式名称；此外如果要加上一些参数，可以在后面写.option("key","value")
    // 这些格式有 csv   format   jdbc   json   load   option   options   orc   parquet   schema   table   text   textFile

    //这样写其实是省略了.format()，因为spark默认读写文件类型是parquet类型，可以修改conf.set("spark.sql.sources.default","json")来改变
    val df: DataFrame = spark.read.load("out")
    //    df.show()
    val df2: DataFrame = spark.read.json("input/bbb.json")
    //    df2.show()
    val df3: DataFrame = spark.read.format("json").load("input/bbb.json")
    //    df3.show()
    val df4: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/chenlong")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "my_user")
      .load()
    //    df4.show()


    // 写数据
    // 默认保存为parquet文件
    df2.write.save("out")

    // 可以指定为保存格式，直接保存，不需要再调用save了，修改conf.set("spark.sql.sources.default","json")
    df.write.json("output1")

    // 写入方式
    // 如果文件已经存在则追加
    df.write.mode("append").json("output1")

    // 如果文件已经存在则忽略
    df.write.mode("ignore").json("output1")

    // 如果文件已经存在则覆盖
    df.write.mode("overwrite").json("output1")

    // 默认是error
    df.write.mode("error").json("output1")

    // 写入mysql
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop101:3306/chenlong")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "my_user")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

}
