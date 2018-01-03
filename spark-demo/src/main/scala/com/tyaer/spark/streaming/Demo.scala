package com.tyaer.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

/**
  * Created by Twin on 2017/7/26.
  */
object Demo {



  def main(args: Array[String]): Unit = {
    // Create a StreamingContext with a local master
    val sparkConf = new SparkConf().setAppName("DelOldrecords")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create a DStream that will connect to serverIP:serverPort, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print a few of the counts to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate


//    val sparkConf = new SparkConf().setAppName("DelOldrecords").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//    val unit = ssc.fileStream("file/word")
//    val words = unit.flatMap(_.split(" "))
//
//    ssc.start() // Start the computation
//    ssc.awaitTermination() // Wait for the computation to terminate


  }

  @Test
  def file: Unit ={
    val sparkConf = new SparkConf().setAppName("DelOldrecords")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val unit = ssc.fileStream("file/word")
//    ssc.queueStream()

    println(unit)
  }
}
