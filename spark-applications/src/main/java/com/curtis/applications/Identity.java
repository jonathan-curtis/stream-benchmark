package com.curtis.applications;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.curtis.benchmarking.Benchmark;

import scala.Tuple2;

public class Identity implements Benchmark<ConsumerRecord<String, String>>{

	@Override
	public JavaDStream<Tuple2<ConsumerRecord<String, String>, Long>> process(
			JavaDStream<ConsumerRecord<String, String>> messages) {
		
		JavaDStream<Tuple2<ConsumerRecord<String, String>, Long>> results = messages.map(x -> { 
			return new Tuple2<ConsumerRecord<String, String>, Long>(x, x.timestamp());
		});
		return results;
	}

}
