package com.curtis.benchmarking;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.curtis.utils.SparkBenchmarkingUtils;

import scala.Tuple2;

public class BenchmarkingApp {
	
	public static final String GROUP_ID = "spark-benchmarking";

	@SuppressWarnings("unchecked")
	public static void main(String...args) throws InterruptedException {
		
		if(args.length != 5) {
			throw new IllegalArgumentException("5 arguments are required to execute a benchmark:\n" +
					"arg0 = benchmark to run - {Identity, Extraction, Grep, WordCount}\r\n" + 
					"arg1 = duration of batch interval\r\n" + 
					"arg2 = Kafka host name and port\r\n" + 
					"arg3 = input topic\r\n" + 
					"arg4 = output topic");
		}
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[2]);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Collection<String> topics = Arrays.asList(args[3]);

		SparkContext sc = SparkContext.getOrCreate();
		JavaStreamingContext jssc = new JavaStreamingContext(new StreamingContext(sc, Durations.seconds(Integer.valueOf(args[1]))));
		
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
				jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topics, kafkaParams));
		
		JavaDStream<Tuple2<?, Long>> results = BenchmarkFactory.getBenchmark(args[0]).process(messages);
		
		SparkBenchmarkingUtils.emitMetrics(results, args[2],  args[4]);

		jssc.start();
		jssc.awaitTermination();
	}
}
