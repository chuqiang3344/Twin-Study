package com.tyaer.spark.word

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Twin on 2017/4/27.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("aa").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file/word/input.txt");

    println("===============")
    rdd.foreach(println)

    val mapRdd = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //同时用RDD工作，如果想了解当前的RDD，那么可使用下面的命令。 它会告诉你关于当前RDD及其依赖调试的描述。

    println("===============mapRdd.toDebugString"+mapRdd.toDebugString)
    //可以使用 persist() 或 cache() 方法标记一个RDD。在第一次计算的操作，这将被保存在存储器中的节点上。使用下面的命令来存储中间转换在内存中。
    mapRdd.cache()
    //应用动作(操作)，比如存储所有的转换结果到一个文本文件中。saveAsTextFile(“”)方法字符串参数是输出文件夹的绝对路径。试试下面的命令来保存输出文本文件。在下面的例子中， ‘output’ 的文件夹为当前位置。
//    mapRdd.saveAsTextFile("output")
    val foreach = mapRdd.collect().foreach(println)

    println("===============")
    mapRdd.filter {
      case (_, count) => {
        if (count > 5) {
          true
        } else {
          false
        }
      }
    }.collect().foreach(println)

    println("1===============")

    println("排序===============")
    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.sortByKey().collect().foreach(println)

    /**
      * 交换位置
      */
    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.collect().foreach(println)

    println("===============")


    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.reduceByKey(_ + _).collect().foreach(println)
    //      .reduceByKey((x,y)=>x+y).collect().foreach(println)


    println("===============")

    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.reduceByKey(_ + "asd" + _).collect().foreach(println)

    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.reduceByKey(_ + "xx" + _).collect().foreach(println)

    mapRdd.map {
      case (key, count) => {
        (count, key)
      }
    }.reduceByKey(_ + "+++" + _).collect().foreach(println)
    //    mapRdd.map {
    //      case (key, count) => {
    //        (count, key)
    //      }
    //    }.reduce(_ + _).collect().foreach(println)

    //    map.reduce {
    //      case (x, y) => {
    //        val z = x + y
    //        z
    //      }
    //    }
    //
    //    map.reduceByKey((x, y) = (x + y))

    //    val map1 = rdd.flatMap(_.split(" ")).map((_, 1)).reduce((x, y) => x + y)
    //    val map1 = rdd.flatMap(_.split(" ")).map((_, 1)).reduce{
    //      case (_,1)=>{
    //
    //      }
    //    }
    //.groupByKey().map(case(x,y)=>(x,y)).foreach(println)


    sc.stop()
  }


}
