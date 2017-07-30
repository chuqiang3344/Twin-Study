package com.tyaer.spark.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by izhonghong on 2017/3/7.
*/
object HBaseAPI {

  def getHbaseConf() ={
    val hbaseConf = HBaseConfiguration.create()
//    conf.addResource("./configure/hbase-site.xml")
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
////    conf.set("hbase.zookeeper.quorum", "test12,test13,test15")
//    conf.setInt("hbase.client.scanner.caching", 10000)
//    conf.setBoolean("hbase.cluster.distributed", true)
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryoserializer.buffer.max","2048")
//    conf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "120000");

    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.setBoolean("hbase.cluster.distributed", true)
    hbaseConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    hbaseConf.setInt("spark.kryoserializer.buffer.max", 2046)
    hbaseConf.setInt("hbase.client.pause", 50)
    hbaseConf.setInt("hbase.client.retries.number", 20)
    hbaseConf.setInt("hbase.client.scanner.caching", 500) //最大读取条数
    hbaseConf.setLong("hbase.rpc.timeout", 120000)
    hbaseConf.setLong("hbase.client.operation.timeout", 6000)
    hbaseConf.setLong("hbase.client.scanner.timeout.period", 150000)

    hbaseConf
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


  def createVtTopicScan():Scan={
    val scan = new Scan()
    val format:SimpleDateFormat=  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val starttime:Long= format.parse("2016-03-11 00:00:00").getTime()
    val endtime:Long = format.parse("2016-03-12 00:00:00").getTime()
    scan.setTimeRange(starttime,endtime)
    scan
  }

  def createVtWeiboScan():Scan={
    val scan = new Scan()
    val format:SimpleDateFormat=  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val starttime:Long= format.parse("2016-01-11 00:00:00").getTime()
    val endtime:Long = format.parse("2016-12-12 00:00:00").getTime()
     scan.setTimeRange(starttime,endtime)
     scan
  }

  def createTable(tableName : String, columnFamilys : Array[String], conf : Configuration): Any ={
      val hAdmin =  new HBaseAdmin(conf)
      if (hAdmin.tableExists(tableName)) {
        println("table={} has exists.", tableName);
      } else {
        // 新建一个表的描述
        val tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        // 在描述里添加列族
        for (columnFamily: String <- columnFamilys) {
          tableDesc.addFamily(new HColumnDescriptor(columnFamily))
        }
        // 根据配置好的描述建表
        hAdmin.createTable(tableDesc);
        println("create table={} success.", tableName);
      }
      if (hAdmin != null) {
        try {
          hAdmin.close()
        } catch {
          case e: Exception => println("close HBaseAdmin has exception={}", e.printStackTrace())
        }
      }
    }

  // 删除数据库表所有记录
 def deleteAllRecords(tableName:String, conf : Configuration) : Unit = {
    // 新建一个数据库管理员
      var hAdmin =  new HBaseAdmin(conf)
      if (hAdmin.tableExists(tableName)) {
        // 关闭一个表
        var table = TableName.valueOf(tableName)
        hAdmin.disableTable(table)
        hAdmin.truncateTable(table, true)//自动enable
//        hAdmin.enableTable(tableName)
        println("clean table={} success.", tableName)
      } else {
        println("this table={} not exist.", tableName)
      }

      if (hAdmin != null) {
        try {
          hAdmin.close()
          hAdmin = null
        } catch{
          case e : Exception =>  println("close HBaseAdmin has exception={}", e.printStackTrace())
        }
     }
  }

  // 删除数据库表所有记录
  def deleteTable(tableName:String, conf : Configuration) : Unit = {
    // 新建一个数据库管理员
    var hAdmin =  new HBaseAdmin(conf)
    if (hAdmin.tableExists(tableName)) {
      // 关闭一个表
      hAdmin.disableTable(tableName)
      var table = TableName.valueOf(tableName)
      hAdmin.deleteTable(table)
      println("delete table={} success.", tableName)
    } else {
      println("this table={} not exist.", tableName)
    }

    if (hAdmin != null) {
      try {
        hAdmin.close()
        hAdmin = null
      } catch{
        case e : Exception =>  println("close HBaseAdmin has exception={}", e.printStackTrace())
      }
    }
  }

  def getHBaseVtTopicData(sc:SparkContext):RDD[(String,String,String)]={
    val conf = getHbaseConf();
    conf.set(TableInputFormat.INPUT_TABLE, "vt_topic")
    conf.set(TableInputFormat.SCAN, convertScanToString(createVtTopicScan()))

    val ShangHaiRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

     val ShangHaiCount = ShangHaiRDD.count()
     ShangHaiRDD.cache()

    val subid=Set("558","559","560","565","566","567","568","578","580")
    val ShangHai= ShangHaiRDD.map{ case (_, result) =>
      val formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss:SSS")
      val sub_id=Bytes.toString(result.getValue("fn".getBytes, "sub_id".getBytes))
      val mid = Bytes.toString(result.getValue("fn".getBytes, "mid".getBytes))
      val text = Bytes.toString(result.getValue("fn".getBytes, "text".getBytes))
      val created_at = Bytes.toString(result.getValue("fn".getBytes, "created_at".getBytes))
      (sub_id,mid,text)
    }.filter{case (sub_id,mid,text) => subid.contains(sub_id) }
    ShangHai
  }

  def getHBaseVtWeiboData(sc:SparkContext):RDD[(String,String,String)]={
    val conf = getHbaseConf();
    conf.set(TableInputFormat.INPUT_TABLE, "vt_weibo")
    conf.set(TableInputFormat.SCAN, convertScanToString(createVtWeiboScan()))

    val totalArticleRDD = sc.newAPIHadoopRDD(conf,
                  classOf[TableInputFormat],
                  classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                  classOf[org.apache.hadoop.hbase.client.Result])
    val totalArticleCount = totalArticleRDD.count()

     totalArticleRDD.cache()
     val totalArticle= totalArticleRDD.map{ case (_, result) =>
      val formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss:SSS")
      val sub_id=Bytes.toString(result.getValue("fn".getBytes, "sub_id".getBytes))
      val mid = Bytes.toString(result.getValue("fn".getBytes, "mid".getBytes))
      val text = Bytes.toString(result.getValue("fn".getBytes, "text".getBytes))
      (sub_id,mid,text)
    }.filter{case(sub_id,mid,text) => !"".equals(text)&&null!=text&&text.length>30&&text.length<500}
    totalArticle
  }

  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext("local", "ShangHaiOnHive")
//    getHBaseVtTopicData(sc)
    val conf = HBaseAPI.getHbaseConf()
    val outputTabel = "sina_weibo_user_test"
    HBaseAPI.createTable(outputTabel,Array("fn"),conf)

  }
}
