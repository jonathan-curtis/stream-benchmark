package com.curtis.applications;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONException;
import org.json.JSONObject;

import com.curtis.benchmarking.Benchmark;
import com.curtis.utils.ExtractKafkaTimestamp;

public class Grep implements Benchmark<Boolean> {

	@Override
	public DataStream<Tuple2<Boolean, Long>> process(DataStream<String> messages) {
		DataStream<Boolean> extractedTextValues = messages.map(x -> {
			JSONObject jsonObject = new JSONObject(x);
			String text;
			try {
				text = jsonObject.getString("text");
			} catch (JSONException jse) {
				// Not a Tweet object so setting text to blank
				text = "";
			}

			return text.contains("#");
		});
		
		DataStream<Tuple2<Boolean, Long>> results = extractedTextValues.process(new ExtractKafkaTimestamp<Boolean>());
		
		return results;
	}

}
