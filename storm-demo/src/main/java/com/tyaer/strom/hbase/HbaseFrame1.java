package com.tyaer.strom.hbase;


import org.apache.hadoop.hbase.client.Durability;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Twin on 2017/6/18.
 */
public class HbaseFrame1 {
    private static final Logger logger = Logger.getLogger(HbaseFrame1.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String zks = "test231:2181,test233:2181,test234:2181";
        String topic = "topic_test";

        String zkRoot = "/storm"; // default zookeeper root configuration for storm
        String id = "test1";

        // Storm Tuple中的两个Field，分别叫做word 和 count
        Fields fields = new Fields("word", "count");

// 定义HBase配置相关和Kerberos相关
        String hBaseConfigKey = "config_key";
        System.setProperty("java.security.krb5.realm", "HADOOP.QIYI.COM");
        System.setProperty("java.security.krb5.kdc", "kerberos-hadoop-dev001-shjj.qiyi.virtual");

//载入HBase和Kerberos相关配置，Config对象是来自backtype.storm.Config 类
        Config conf = new Config();
        conf.setDebug(true);
        Map<String, String> hBaseConfigMap = new HashMap<String, String>();
        hBaseConfigMap.put(HBaseSecurityUtil.STORM_KEYTAB_FILE_KEY, "/home/yeweichen/yeweichen.keytab");
        hBaseConfigMap.put(HBaseSecurityUtil.STORM_USER_NAME_KEY, "yeweichen@HADOOP.QIYI.COM");
        conf.put("config_key", hBaseConfigMap);

// 定义Trident拓扑，从Kafka中获取数据
        TridentTopology tridentTopology = new TridentTopology();
        BrokerHosts zk = new ZkHosts(zks);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic);
//        spoutConf.forceFromStart = true;
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

//定义HBase的Mapper，指定“word”字段的内容作为rowkey，列族名为cf
        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
                .withColumnFamily("cf")
                .withColumnFields(new Fields("word"))
                .withColumnFields(new Fields("count"))
                .withRowKeyField("word");

// LogCollect就是自定义的Mapper
        HBaseValueMapper rowToStormValueMapper = new LogCollectMapper();

//定义投影类，加入cf列族中的word和count两个列
        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "word"));
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("cf", "count"));

//定义HBaseState类的属性类Option
        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(hBaseConfigKey)
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withProjectionCriteria(projectionCriteria)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName("storminput");

//使用工厂方法和Option生成HBaseState对象
        StateFactory factory = new HBaseStateFactory(options);

//定义Stream，从Kafka中读出的数据，使用AddTimeFunction方法把它生成word和field两个字段，然后把他们写入HBase,如上面定义的，word字段作为row key
//        tridentTopology.newStream("myspout", spout).each(new Fields("str"), new AddTimeFunction(), new Fields("word", "count"))
//                .partitionPersist(factory, fields, new HBaseUpdater(), new Fields());

// 提交拓扑
        StormSubmitter.submitTopology(args[0], conf, tridentTopology.build());

    }

    /**
     * 自定义tuple与hbase数据行的映射
     *
     * @author adam
     */
    public class MyHBaseMapper implements HBaseMapper {

        public ColumnList columns(Tuple tuple) {

            ColumnList cols = new ColumnList();
            //参数依次是列族名，列名，值
            cols.addColumn("c1".getBytes(), "str".getBytes(), tuple.getStringByField("str").getBytes());
            cols.addColumn("c2".getBytes(), "num".getBytes(), tuple.getStringByField("num").getBytes());

            return cols;
        }

        public byte[] rowKey(Tuple tuple) {
            //根据tuple指定id作为rowkey
            return tuple.getStringByField("id").getBytes();
        }

    }

}
