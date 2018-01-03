package com.tyaer.strom.hbase;


import org.apache.hadoop.hbase.client.Durability;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseBolt;
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
public class HbaseFrame2 {
    private static final Logger logger = Logger.getLogger(HbaseFrame2.class);

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        HBaseMapper mapper = new MyHBaseMapper();
        HBaseBolt hbaseBolt = new HBaseBolt("stormTest", mapper).withConfigKey("hbase.conf");
        Config conf = new Config();
        conf.setDebug(true);
        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir", "HBASE根目录");
        conf.put("hbase.conf", hbConf);
    }

    /**
     * 自定义tuple与hbase数据行的映射
     * @author adam
     *
     */
    public static class MyHBaseMapper implements HBaseMapper {

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
