package com.tyaer.strom.demo1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by Twin on 2017/6/17.
 */


public class FirstTopo {

    public static void main(String[] args) throws Exception {
        System.out.println("asd");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSpout());
        builder.setBolt("bolt", (IBasicBolt) new SenqueceBolt()).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(false);
//        String zks = "test231:2181,test233:2181,test234:2181";
//        BrokerHosts brokerHosts = new ZkHosts(zks);
//        SpoutConfig spoutConfig = new SpoutConfig();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            System.out.println("本地模式");
            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("firstTopo", conf, builder.createTopology());
            cluster.submitTopology("firstTopo", conf, builder.createTopology());
            Utils.sleep(3000);
            cluster.killTopology("firstTopo");
            cluster.shutdown();
        }
    }
}
