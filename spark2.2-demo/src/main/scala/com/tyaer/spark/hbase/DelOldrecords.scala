package com.tyaer.spark.hbase

import java.util.Calendar

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/4/12.
  */
object DelOldrecords extends Serializable {

  @transient
  val tmpConf = HBaseAPI.getHbaseConf()
  @transient
  var tableName = "sina_weibo_user_dt_00"
  @transient
  val conn = ConnectionFactory.createConnection(tmpConf)

  //  val table = conn.getTable(TableName.valueOf(tableName))

  var table: Table = null;

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      tableName = args(0)
    }
    //    tableName = "sina_weibo_user_dt_52"
    table = conn.getTable(TableName.valueOf(tableName))
    val sparkConf = new SparkConf().setAppName("DelOldrecords")
    if (System.getProperty("os.name").toLowerCase.contains("windows")) {
      sparkConf.setMaster("local[*]")
    } else {
      sparkConf.setMaster("yarn-client")
    }
    val sc = new SparkContext(sparkConf)
    tmpConf.set(TableInputFormat.INPUT_TABLE, tableName)
    var scan = new Scan();
    scan.setCaching(500)
    scan.setCacheBlocks(false)
    scan.setFilter(new FirstKeyOnlyFilter)
    //时间过滤，删除历史记录
    scan.setTimeRange(0L, Calendar.getInstance().getTimeInMillis - 18 * 3600 * 1000);
    //    scan.setTimeRange(0L, Calendar.getInstance().getTimeInMillis - 5 * 60 * 1000);
    tmpConf.set(TableInputFormat.SCAN, HBaseAPI.convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(tmpConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count = hBaseRDD.count
    println(table.getName + " ====================需要删除记录数：" + count)

    //    val del = new Delete(Bytes.toBytes("ffff_5483049799"))
    //    table.delete(del)
    hBaseRDD.foreach {
      case (_, result) => {
        val row = result.getRow
        println(new String(row) + "\tgetTimestamp:" + result.rawCells()(0).getTimestamp)
        val del = new Delete(row)
        table.delete(del)
      }
    }

    table.close()
    conn.close()

    sc.stop()
    println(table.getName + " ====================删除完毕：" + count)
  }


}
