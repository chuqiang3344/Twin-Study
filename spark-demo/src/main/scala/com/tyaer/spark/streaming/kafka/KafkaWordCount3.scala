package com.tyaer.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/9/16.
  */
object KafkaWordCount3 {

  val batchDuration = 2;

  val checkpointDirectory = "D:/checkpoints"

  // 通过函数来创建或者从已有的checkpoint里面构建StreamingContext
  def functionToCreateContext(): StreamingContext = {
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
      "auto.offset.reset" -> "smallest"
      //      "auto.offset.reset" -> "largest"
    )
    // 直连方式拉取数据，这种方式不会修改数据的偏移量，需要手动的更新
    val rdds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics)).map(_._2)
    //   val lines = KafkaUtils.createStream(ssc, "m1:2181,m2:2181,m3:2181", "spark", topics).map(_._2)

    val ds1 = rdds.flatMap(_.replace("\n", "").split(" ")).filter(!_.isEmpty).map((_, 1))

    val ds2 = ds1.updateStateByKey[Int]((x: Seq[Int], y: Option[Int]) => {
      Some(x.sum + y.getOrElse(0))
    })

    ds2.count().print()
    ds2.print()

    ssc.checkpoint(checkpointDirectory) // 设置在HDFS上的checkpoint目录
    //设置通过间隔时间，定时持久checkpoint到hdfs上
    rdds.checkpoint(Seconds(batchDuration * 5))//一般为批处理间隔的5到10倍是比较好的一个方式。

    rdds.foreachRDD(rdd => {
      //可以针对rdd每次调用checkpoint
      //注意上面设置了，定时持久checkpoint下面这个地方可以不用写
      rdd.checkpoint()
    }
    )
    //返回ssc
    ssc
  }

  def main(args: Array[String]): Unit = {
    // 创建context
    val context = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    // 启动流计算
    context.start()
    context.awaitTermination()

  }

}