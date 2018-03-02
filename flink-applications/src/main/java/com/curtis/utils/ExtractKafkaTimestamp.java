package com.curtis.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class ExtractKafkaTimestamp<T> extends ProcessFunction<T, Tuple2<T, Long>> {

	private static final long serialVersionUID = 1934841678545385659L;

	public void processElement(T element, ProcessFunction<T, Tuple2<T, Long>>.Context context, Collector<Tuple2<T, Long>> collector)
			throws Exception {
		collector.collect(new Tuple2<T, Long>(element, context.timestamp()));
	}
}