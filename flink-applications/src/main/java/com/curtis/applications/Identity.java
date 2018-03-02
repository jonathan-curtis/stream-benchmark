package com.curtis.applications;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.curtis.benchmarking.Benchmark;
import com.curtis.utils.ExtractKafkaTimestamp;

public class Identity implements Benchmark<String> {

	@Override
	public DataStream<Tuple2<String, Long>> process(DataStream<String> messages) {
		return messages.process(new ExtractKafkaTimestamp<String>());
	}

}
