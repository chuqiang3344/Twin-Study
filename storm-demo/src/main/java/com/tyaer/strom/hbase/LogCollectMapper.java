package com.tyaer.strom.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by Twin on 2017/6/20.
 */
public class LogCollectMapper implements HBaseValueMapper {
    @Override
    public List<Values> toValues(ITuple iTuple, Result result) throws Exception {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
