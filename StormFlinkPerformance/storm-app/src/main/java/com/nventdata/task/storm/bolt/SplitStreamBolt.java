package com.nventdata.task.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * @author kidio
 * 
 * class SplitStreamBolt to split the input stream into 3 output streams
 *	based on `random` field
 *
 */
public class SplitStreamBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream("random1", new Fields("random1", "message"));
    	declarer.declareStream("random2", new Fields("random3", "message"));
    	declarer.declareStream("random3", new Fields("random3", "message"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String streamId = tuple.getStringByField("random");
    	collector.emit (streamId, new Values(tuple.getStringByField("random"),tuple.getStringByField("message")));
    }
}
