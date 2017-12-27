package com.tyaer.spark_java.streaming.kafka;

import com.tyaer.spark_java.zookeeper.ZKUtils;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by Twin on 2017/9/16.
 */
public class Demo1 {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Logger logger = LogManager.getLogger(Demo1.class);
    private static DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static int maxSendSize = 50;

    private Demo1() {
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 6) {
            System.err.println("Usage: ArticleDistributionJob <brokerList> <group> <topic> <totalPartitions> <numThreads> <duration>");
            args = new String[]{"test11:9092,test12:9092,test13:9092", "zcq_test", "exclude_weibo_data", "1", "4", "2000"};
//            System.exit(1);
        }

        System.out.println(ArrayUtils.toString(args));
        String brokerList = args[0];
        final String group = args[1];
        String topic = args[2];
        int totalPartitions = Integer.parseInt(args[3]);
        int numThreads = Integer.parseInt(args[4]);
        int duration = Integer.parseInt(args[5]);//批处理间隔时间 ms

        SparkConf sparkConf = new SparkConf().setAppName("ArticleDistributionJob");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");//【实战篇】如何优雅的停止你的 Spark Streaming Application - 简书http://www.jianshu.com/p/234c85f5ec3f
        // Create the context with 2 seconds batch size
        //sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
        //jssc.sc().addJar("hdfs:///app/hduser1301/lib/hbase-client-1.2.2.jar");
        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        topicMap.put(topic, numThreads);


        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokerList);


        kafkaParams.put("group.id", group);

        kafkaParams.put("session.timeout.ms", "30000");
        kafkaParams.put("request.timeout.ms", "60000");


        java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets = new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
        Map<Integer, Long> partitionOffsets = ZKUtils.getPartitionOffset(group, topic);
        Map<Integer, Long> earlistOffsets = null;
        try {
            earlistOffsets = KafkaTest.getEarlistOffset(topic, totalPartitions);
        } catch (Exception e) {
            logger.error("failed to get the partitions' offset, the topic:" + topic + " may doesn't exist");
        }

        if (null != partitionOffsets && partitionOffsets.size() > 0) {
            for (Integer partition : partitionOffsets.keySet()) {
                long offset = partitionOffsets.get(partition);
                long earlistOffset = earlistOffsets.get(partition);
                logger.info("partition:" + partition + ",offset:" + offset + ",earlistOffset:" + earlistOffset);
                System.out.println("partition:" + partition + ",offset:" + offset + ",earlistOffset:" + earlistOffset);
                if (earlistOffset > offset) {
                    offset = earlistOffset;
                }
                fromOffsets.put(new TopicAndPartition(topic, partition), offset);
            }
        } else {
            for (int i = 0; i < totalPartitions; i++) {
                fromOffsets.put(new TopicAndPartition(topic, i), earlistOffsets.get(i));
            }
        }
        System.out.println("kafkaParams:" + kafkaParams);
        System.out.println("fromOffsets:" + fromOffsets);
        System.out.println("partitionOffsets:" + partitionOffsets);


        //create the direct stream, with witch we can maintain the offset in zookeeper by ourselves
        JavaInputDStream<Row> stream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, Row.class, kafkaParams, fromOffsets,
                new Function<MessageAndMetadata<String, String>, Row>() {
                    public Row call(MessageAndMetadata<String, String> v1)
                            throws Exception {
                        String message = v1.message();

                        return RowFactory.create(message);
                    }
                });


        //final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jssc.sc());


        System.out.println("=========================lines count:" + stream.count());

        stream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rowJavaRDD) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rowJavaRDD.rdd()).offsetRanges();
                JavaRDD<Row> filter = rowJavaRDD.filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        return row != null && row.get(0) != null;
                    }
                });
                filter.flatMap(new FlatMapFunction<Row, Object>() {
                    @Override
                    public Iterator<Object> call(Row row) throws Exception {
                        return null;
                    }
                });
//                return null;
            }
        });

        System.out.println("finished..");

//        stream.print();

        jssc.start();

        jssc.awaitTermination();
    }
}
