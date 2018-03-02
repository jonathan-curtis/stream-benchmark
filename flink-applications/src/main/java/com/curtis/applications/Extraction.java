package com.curtis.applications;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONException;
import org.json.JSONObject;

import com.curtis.benchmarking.Benchmark;
import com.curtis.utils.ExtractKafkaTimestamp;

public class Extraction implements Benchmark<String> {

	@Override
	public DataStream<Tuple2<String, Long>> process(DataStream<String> messages) {
		DataStream<String> extractedTextValues = messages.map(x -> {
			JSONObject jsonObject = new JSONObject(x);
			String text;
			try {
				text = jsonObject.getString("text");
			} catch (JSONException jse) {
				// Not a Tweet object so setting text to blank
				text = "";
			}

			return text;
		});
		
		DataStream<Tuple2<String, Long>> results = extractedTextValues.process(new ExtractKafkaTimestamp<String>());
		
		return results;
	}
}
