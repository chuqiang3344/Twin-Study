package com.tyaer.spark.base

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by Twin on 2017/12/28.
  */
class Study {

  @Test
  def t1(): Unit = {
    val sparkConf = new SparkConf().setAppName("t1").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
//    val rdd = sc.textFile("file/word/input.txt")
    val rdd = sc.textFile("D:\\IdeaProjects\\~Twin\\Twin-Study\\file\\word\\input.txt")
    println(rdd)
    val tuples = rdd.flatMap(line => line.split("_")).map((_, 1)).reduceByKey(_ + _).collect()
      tuples.foreach(println)
  }

  def getsc(): StreamingContext ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey")
    val context = new StreamingContext(sparkConf, Seconds(2))
    context
  }

  @Test
  def transformat(): Unit ={
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey")
    val context = new StreamingContext(sparkConf, Seconds(2))
    val spamInfoRDD = context.sparkContext.newAPIHadoopRDD(sparkConf) // RDD containing spam information

    val cleanedDStream = wordCounts.transform(rdd => {
      rdd.join(spamInfoRDD).filter(...) // join data stream with spam information to do data cleaning
      ...
    })

    unit.transform(rdd=>{

    })
  }

  @Test
  def UpdateStateByKey: Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKey")
    val context = new StreamingContext(sparkConf, Seconds(2))
    val lines = context.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))

    import org.apache.spark.streaming.State
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
  }

}
