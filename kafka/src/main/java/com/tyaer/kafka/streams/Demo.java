package com.tyaer.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Twin on 2017/7/26.
 */
public class Demo {
    public static void main(String[] args) {
//        TopologyBuilder topologyBuilder = new TopologyBuilder();
//        new KafkaStreams(topologyBuilder);

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");

//        String servers = "test11:9092,test12:9092,test13:9092";
        String servers = "test231:9092,test233:9092,test234:9092";
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
//        builder.stream("my-input-topic").mapValues(value -> value.toString()).to("my-output-topic");//lambda表达式
        KStream<Object, Object> topic_test1 = builder.stream("topic_test");
        KStream<Object, String> topic_test = topic_test1.mapValues(new ValueMapper<Object, String>() {
            @Override
            public String apply(Object value) {
                //修改内容
                String string = value.toString();
                System.out.println(string);
                return string;
            }
        });
        topic_test.to("topic_test1");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
