package com.tyaer.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/6/7.
  */
object MysqlDemo {

  val NUM_SAMPLES: Int = 1000

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")


    // Creates a DataFrame based on a table named "people"
    // stored in a MySQL database.
    Class.forName("com.mysql.jdbc.Driver")
    val sqlContext = SQLContext.getOrCreate(sc)
    val url =
      "jdbc:mysql://119.145.230.3:53306/zdasss"
    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "dispute_event_1")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "izhonghong@2016root123")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema() //数据结构
    //    df.show()

    df.registerTempTable("names")
    df.sqlContext.sql("select * from names").collect.foreach(println)

  }

}
