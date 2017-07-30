package com.tyaer.spark.hbase

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/4/28.
  */
object TableCount {

  def main(args: Array[String]): Unit = {
    var tableName = "sina_weibo_user"
    if (args.length > 0) {
      tableName = args(0)
    }
    //    val sparkConf = new SparkConf().setAppName("TableCount").setMaster("local[*]")
    val sparkConf = new SparkConf().setAppName("TableCount")
    if (System.getProperty("os.name").toLowerCase.contains("windows")) {
      sparkConf.setMaster("local[*]")
    } else {
      sparkConf.setMaster("yarn-client")
    }
    val sc = new SparkContext(sparkConf)
    val tmpConf = HBaseAPI.getHbaseConf()
    //    tmpConf.addResource("./configure/hbase-site.xml")
    tmpConf.set(TableInputFormat.INPUT_TABLE, tableName)
    var scan = new Scan();
    scan.setCaching(500)
    scan.setCacheBlocks(false)
    scan.setFilter(new FirstKeyOnlyFilter)
    tmpConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    val hBaseRDD = sc.newAPIHadoopRDD(tmpConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("====================记录数：" + hBaseRDD.count)

    sc.stop()

    //    Thread.sleep(1000*60)

  }
}
