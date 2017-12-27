package com.tyaer.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/9/16.
  */
object KafkaWordCount2 {


  def main(args: Array[String]): Unit = {

    val Array(brokerList, group, topics, numThreads) = Array("test11:9092,test12:9092,test13:9092", "zcq_test", "topic_test", "1");

    val conf = new SparkConf()
    conf.setAppName("spark_streaming")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(batchDuration))

    // val topics = Map("spark" -> 2)

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokerList,
      "group.id" -> group,
      //    "auto.offset.reset" -> "smallest"
      "auto.offset.reset" -> "largest"
    )
    // 直连方式拉取数据，这种方式不会修改数据的偏移量，需要手动的更新
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics)).map(_._2)
    //   val lines = KafkaUtils.createStream(ssc, "m1:2181,m2:2181,m3:2181", "spark", topics).map(_._2)

    val ds1 = lines.flatMap(_.replace("\n", "").split(" ")).filter(!_.isEmpty).map((_, 1))

    val ds2 = ds1.updateStateByKey[Int]((x: Seq[Int], y: Option[Int]) => {
      Some(x.sum + y.getOrElse(0))
    })

    ds2.count().print()
    ds2.print()

    ssc.start()
    ssc.awaitTermination()


  }

  val batchDuration = 2;

  val checkpointDirectory = "D:/checkpoints"

}