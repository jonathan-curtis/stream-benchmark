package com.curtis.benchmarking;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;

public interface Benchmark<E> {
	
	public DataStream<Tuple2<E, Long>> process(DataStream<String> messages);
	
}
