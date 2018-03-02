package com.curtis.applications;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.json.JSONException;
import org.json.JSONObject;

import com.curtis.benchmarking.Benchmark;

import scala.Tuple2;

public class Extraction implements Benchmark<String> {

	@Override
	public JavaDStream<Tuple2<String, Long>> process(JavaDStream<ConsumerRecord<String, String>> messages) {
		JavaDStream<Tuple2<String, Long>> results = messages.map(x -> { 
			
			JSONObject jsonObject = new JSONObject(x.value());
			String text;
			try {
				text = jsonObject.getString("text");
			} catch (JSONException jse) {
				// Not a Tweet object so setting text to blank
				text = "";
			}

			return new Tuple2<String, Long>(text, x.timestamp());
		});
		
		return results;
	}
}
