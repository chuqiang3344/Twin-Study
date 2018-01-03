package com.tyaer.strom.frame;


import com.tyaer.strom.kafka.MyKafkaTopology;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Twin on 2017/6/18.
 */
public class KafkaFrame {
    private static final Logger logger = Logger.getLogger(KafkaFrame.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String zks = "test231:2181,test233:2181,test234:2181";
        String topic = "topic_test";

        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConf.forceFromStart = false;
        spoutConf.zkServers = Arrays.asList(new String[]{"test231", "test233", "test234"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter", new Bolt1(), 2).shuffleGrouping("kafka-reader");
//        builder.setBolt("word-counter", new Bolt1()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            System.out.println("###sleep.......");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cluster.shutdown();
        }
    }

    public static class Spout implements IRichSpout {
        private static final long serialVersionUID = 1L;
        private static final Logger logger = Logger.getLogger(Spout.class);
        private SpoutOutputCollector spoutOutputCollector;
        private boolean completed = false;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            //获取创建Topology时指定的要读取的文件路径
            Object o = map.get("");
            //初始化发射器
            this.spoutOutputCollector = spoutOutputCollector;
        }

        /**
         * 这是Spout最主要的方法，在这里我们读取文本文件，并把它的每一行发射出去（给bolt）
         * 这个方法会不断被调用，为了降低它对CPU的消耗，当任务完成时让它sleep一下
         **/
        @Override
        public void nextTuple() {

        }

        @Override
        public void close() {

        }

        @Override
        public void activate() {

        }

        @Override
        public void deactivate() {

        }


        @Override
        public void ack(Object msgId) {
            System.out.println("OK:" + msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println("FAIL:" + msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class Bolt1 implements IRichBolt {
        private static final Logger logger = Logger.getLogger(Bolt1.class);
        String thisComponentId;
        int thisTaskId;
        private OutputCollector outputCollector;
        private static AtomicInteger atomicInteger=new AtomicInteger(0);

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
            this.thisComponentId = topologyContext.getThisComponentId();
            this.thisTaskId = topologyContext.getThisTaskId();
        }

        @Override
        public void execute(Tuple tuple) {
            String str = tuple.getString(0);
            logger.info(str);
            logger.info("计数器："+atomicInteger.incrementAndGet());
            // 确认成功处理一个tuple
            //如果不关心数据是否丢失（例如数据统计分析的典型场景），不要启用ack机制。
            outputCollector.ack(tuple);
        }

        /**
         * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
         * 因为这只是个Demo，我们用它来打印我们的计数器
         */
        @Override
        public void cleanup() {
            logger.info("-- Word Counter [" + thisComponentId + "-" + thisTaskId + "] --");

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//            outputFieldsDeclarer.declare(new Fields("word")); //分组ID
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
