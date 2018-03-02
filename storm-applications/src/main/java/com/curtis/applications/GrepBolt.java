package com.curtis.applications;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GrepBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("grep", "timestamp"));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {

		String text = tuple.getStringByField("text");
		String containsHashTag = Boolean.toString(text.contains("#"));

		outputCollector.emit(new Values(containsHashTag, tuple.getLongByField("timestamp")));
	}
}
