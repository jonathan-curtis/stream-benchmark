package com.curtis.applications;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.json.JSONException;
import org.json.JSONObject;

import com.curtis.benchmarking.Benchmark;

import scala.Tuple2;

public class WordCount implements Benchmark<Map<String, Integer>> {

	@Override
	public JavaDStream<Tuple2<Map<String, Integer>, Long>> process(JavaDStream<ConsumerRecord<String, String>> messages) {

		JavaDStream<Tuple2<Map<String, Integer>, Long>> results = messages.map(x -> { 
			JSONObject jsonObject = new JSONObject(x.value());
			String text;
			try {
				text = jsonObject.getString("text");
			} catch (JSONException jse) {
				// Not a Tweet object so setting text to blank
				text = "";
			}
			Map<String, Integer> count = Stream.of(text.split("\\W+"))
					.collect(Collectors.toMap(w -> w, w -> 1, Integer::sum));
			return new Tuple2<Map<String, Integer>, Long>(count, x.timestamp());
		});
		
		return results;
	}
}
