package com.tyaer.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by Twin on 2017/12/27.
  */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("t1").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = new StringGenerateRDD(sc,"sun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\n", 3)
    rdd.flatMap(_.split("_")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
  }

}
