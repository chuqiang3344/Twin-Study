package com.tyaer.spark.rdd

/**
  * Created by Twin on 2017/12/29.
  */
class SharedVariable {
//  scala> val accum = sc.accumulator(0, "My Accumulator")
//  accum: spark.Accumulator[Int] = 0
//  scala> sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
//    ...
//  10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s
//  scala> accum.value
//  res2: Int = 10
//
//  这个例子利用了内置的整数类型累加器。开发者可以利用子类AccumulatorParam创建自己的累加器类型。AccumulatorParam接口有两个方法：zero方法为你的数据类型提供一个“0 值”（zero value）；addInPlace方法计算两个值的和。例如，假设我们有一个Vector类代表数学上的向量，我们能够如下定义累加器：
//  object VectorAccumulatorParam extends AccumulatorParam[Vector] {
//    def zero(initialValue: Vector): Vector = {
//      Vector.zeros(initialValue.size)
//    }
//    def addInPlace(v1: Vector, v2: Vector): Vector = {
//      v1 += v2
//    }
//  }
//  // Then, create an Accumulator of this type:

}
