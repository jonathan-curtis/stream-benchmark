package com.curtis.applications;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ExtractTimestampPrepareKafkaOutputBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -274941560080891442L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		Long stimulusTime = (Long) tuple.getLongByField("timestamp");
		Long egressTime = Instant.now().toEpochMilli();
		List<Long> metrics = Arrays.asList(stimulusTime, egressTime);

		outputCollector.emit(new Values(metrics.toString()));
	}
}
