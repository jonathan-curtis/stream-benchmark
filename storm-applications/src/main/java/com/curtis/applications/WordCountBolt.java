package com.curtis.applications;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8907322100975135923L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wordcount", "timestamp"));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		
		String text = tuple.getStringByField("text");

		Map<String, Integer> count = Stream.of(text.split("\\W+"))
				.collect(Collectors.toMap(w -> w, w -> 1, Integer::sum));

		outputCollector.emit(new Values(count.toString(), tuple.getLongByField("timestamp")));
	}
	
}
