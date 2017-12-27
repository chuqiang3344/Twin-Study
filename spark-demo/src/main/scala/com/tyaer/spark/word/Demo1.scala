package com.tyaer.spark.word

import org.apache.spark.{SparkConf, SparkContext}

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

    sc.textFile("file/word/input.txt");
  }
}
