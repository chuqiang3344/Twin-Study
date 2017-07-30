package com.tyaer.spark_java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by Twin on 2017/7/26.
 */
public class Demo1 {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
//        StreamingExamples.setStreamingLogLevels();

        JavaStreamingContext jssc = new JavaStreamingContext("local[*]",
                "JavaNetworkWordCount", new Duration(10000));
//        jssc.checkpoint(".");//使用updateStateByKey()函数需要设置checkpoint
//        //打开本地的端口9999
//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
//        //按行输入，以空格分隔
//        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)));
//        //每个单词形成pair，如（word，1）
//        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
//        //统计并更新每个单词的历史出现次数
//        JavaPairDStream<String, Integer> counts = pairs.updateStateByKey((values, state) -> {
//            Integer newSum = state.or(0);
//            for(Integer i :values) {
//                newSum += i;
//            }
//            return Optional.of(newSum);
//        });
//        counts.print();
//        jssc.start();
//        jssc.awaitTermination();
    }
}
