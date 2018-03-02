package com.curtis.applications;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONException;
import org.json.JSONObject;

import com.curtis.benchmarking.Benchmark;
import com.curtis.utils.ExtractKafkaTimestamp;

public class WordCount implements Benchmark<Map<String, Integer>> {

	@Override
	public DataStream<Tuple2<Map<String, Integer>, Long>> process(DataStream<String> messages) {
		DataStream<Map<String, Integer>> extractedTextValues = messages.map(x -> {
			JSONObject jsonObject = new JSONObject(x);
			String text;
			try {
				text = jsonObject.getString("text");
			} catch (JSONException jse) {
				// Not a Tweet object so setting text to blank
				text = "";
			}
			
			Map<String, Integer> count = Stream.of(text.split("\\W+"))
					.collect(Collectors.toMap(w -> w, w -> 1, Integer::sum));

			return count;
		}).returns(TypeInformation.of(new TypeHint<Map<String, Integer>>() {}));
		
		DataStream<Tuple2<Map<String, Integer>, Long>> results = 
				extractedTextValues.process(new ExtractKafkaTimestamp<Map<String, Integer>>());
		
		return results;
	}
}
