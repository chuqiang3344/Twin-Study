//package com.tyaer.spark
//
//import com.izhonghong.mission.forward.TaskSend.loadJDBC
//import com.izhonghong.mission.utils.{HDFSUtil, HbaseUtil}
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Scan
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.storage.StorageLevel
//
///**
//  * Created by Zsh on 12/15 0015.
//  */
//object UserStatistics {
//  val dfs = HDFSUtil.getFS()
//  //  val path = "hdfs://test15:8020/jishue9.txt"
//  val path = "hdfs://izhonghong/user/hduser1301/zsh/userstatistics"
//  def main(args: Array[String]): Unit = {
//    HDFSUtil.rm(path,true)
//    val sparkConf = new SparkConf()
//      .setAppName("UserStatistics")
//      //      .setMaster("local[4]")
//      .setMaster("yarn-client")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],classOf[org.apache.hadoop.hbase.client.Result]))
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new SQLContext(sc)
//    val conf =HBaseConfiguration.create
//    conf.set(TableInputFormat.INPUT_TABLE, "crawl:sina_weibo_user")
//    //    conf.set(TableInputFormat.INPUT_TABLE, "sina_weibo_user")
//    //    conf.set("SPARK_LOCAL_DIRS12","/home/tmp")
//    //    println(sparkConf.get("SPARK_LOCAL_DIRS"))
//    //    conf.set(TableInputFormat.INPUT_TABLE, "sina_weibo_user")
//    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//    val scan = new Scan
//    //    scan.addColumn("fn".getBytes,"commentNum".getBytes)
//    //    scan.addColumn("fn".getBytes,"forward_count".getBytes)
//    //    scan.addColumn("fn".getBytes,"mid".getBytes)
//    //    scan.addColumn("fn".getBytes,"subject".getBytes)
//    scan.addColumn("fn".getBytes,"taskType".getBytes)
//    val ScanToString =  HbaseUtil.convertScanToString(scan)
//    conf.set(TableInputFormat.SCAN, ScanToString)
//    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//      .repartition(1000)
//      .persist(StorageLevel.MEMORY_AND_DISK)
//
//    println("rduce:"+hBaseRDD.getNumPartitions)
//    //    println("总数："+hBaseRDD.count())
//    hBaseRDD.map(_._2).map(x=>(Bytes.toString(x.getRow))).map(x=>x.substring(x.indexOf("_")+1)).filter(_!=null).filter(_.length>5).map(x=>{
//      var y = x
//      try {
//        y = x.substring(0, 4)
//      } catch {
//        case e => e.printStackTrace()
//      }
//      (y.substring(0,4),1)
//    }).reduceByKey(_+_)
//      .saveAsTextFile(path)
//    //      .saveAsTextFile("hdfs://izhonghong/user/hduser1301/zsh/userstatistics")
//
//    hBaseRDD.unpersist()
//
//  }
//
//}
