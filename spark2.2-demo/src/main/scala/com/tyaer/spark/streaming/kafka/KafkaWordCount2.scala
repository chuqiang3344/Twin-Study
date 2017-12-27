package com.tyaer.spark.streaming.kafka

import java.util
import java.util.{HashMap, Map}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * Created by Twin on 2017/9/16.
  */
object KafkaWordCount2 {

    def brokerList: _root_.scala.Predef.String = "test11:9092,test12:9092,test13:9092";

    def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>");

      //        System.exit(1)
    }

    //      StreamingExamples.setStreamingLogLevels()

//    val Array(zkQuorum, group, topics, numThreads) = args
//    val Array(zkQuorum, group, topics, numThreads) = Array ("test11:9092,test12:9092,test13:9092","zcq_test" , "topic_test", "1");
    val Array(zkQuorum, group, topics, numThreads) = Array ("test11,test12,test13","zcq_test" , "topic_test", "1");
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    println(topicMap)

      val kafkaParams: util.Map[String, String] = new util.HashMap[String, String]
      kafkaParams.put("metadata.broker.list", brokerList)
      kafkaParams.put("group.id", group)
      kafkaParams.put("session.timeout.ms", "30000")
      kafkaParams.put("request.timeout.ms", "60000")

//    val lines = KafkaUtils.createDirectStream(ssc,kafkaParams,topics);
//    val words = lines.flatMap(
//        _.split(" "))
//    val wordCounts = words.map(x => (x, 1L))
//      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
//    wordCounts.print();

    ssc.start()
    ssc.awaitTermination()
  }

}
