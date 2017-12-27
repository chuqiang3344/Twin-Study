package com.tyaer.spark.word

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by Twin on 2017/12/25.
  */
object Demo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val wordCountsWithReduce = wordPairsRDD.reduceByKey(_ + _)

    wordCountsWithReduce.foreach(println)

    val wordCountsWithGroup = wordPairsRDD.groupByKey().map(t => (t._1, t._2.sum))
    wordCountsWithGroup.foreach(println)
  }

  @Test
  def t1(): Unit ={
  }

}
