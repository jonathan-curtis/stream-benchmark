package com.curtis.benchmarking;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

public interface Benchmark<E> {
	
	public JavaDStream<Tuple2<E, Long>> process(JavaDStream<ConsumerRecord<String, String>> messages);
	
}
