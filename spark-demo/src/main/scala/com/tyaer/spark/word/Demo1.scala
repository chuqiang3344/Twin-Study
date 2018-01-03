package com.tyaer.spark.word

import com.tyaer.spark.rdd.StringGenerateRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by Twin on 2017/12/27.
  */
object Demo1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val array=Array(1,2,3,4,5)
    val rdd = sc.parallelize(array,4)
    //并行集合一个很重要的参数是切片数(slices)，表示一个数据集切分的份数。Spark 会在集群上为每一个切片运行一个任务。你可以在集群上为每个 CPU 设置 2-4 个切片(slices)。正常情况下，Spark 会试着基于你的集群状况自动地设置切片的数目。然而，你也可以通过 parallelize 的第二个参数手动地设置(例如：sc.parallelize(data, 10))。
    val rdd1 = rdd.reduce((a, b) => {
      a + b
    })
    println(rdd1)

    sc.textFile("file/word/input.txt").flatMap(_.split(" ")).collect().foreach(println);
  }

  @Test
  def t1(): Unit ={
    val sparkConf = new SparkConf().setAppName("t1").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = new StringGenerateRDD(sc,"sun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\nsun biao biao\n sun biao biao\n", 3)
    rdd.flatMap(_.split("_")).map((_,1)).reduceByKey(_+_).collect().foreach(println)
  }
}
