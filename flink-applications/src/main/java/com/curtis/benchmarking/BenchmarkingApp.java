package com.curtis.benchmarking;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


public class BenchmarkingApp {

	public static final String GROUP_ID = "flink-benchmarking";
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		if(args.length != 4) {
			throw new IllegalArgumentException("4 arguments are required to execute a benchmark:\n" +
					"arg0 = benchmark to run - {Identity, Extraction, Grep, WordCount}\r\n" + 
					"arg1 = Kafka host name and port\r\n" + 
					"arg2 = input topic\r\n" + 
					"arg3 = output topic");
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties kafkaParams = new Properties();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>(args[2], new SimpleStringSchema(), kafkaParams);			
		
		DataStream<String> stream = env.addSource(consumer);		
				
		DataStream<Tuple2<?, Long>> data = BenchmarkFactory.getBenchmark(args[0]).process(stream);
		
		DataStream<String> metrics = data.map(x -> {
			Long stimulusTime = x.f1;
			Long egressTime = Instant.now().toEpochMilli();
			List<Long> result = Arrays.asList(stimulusTime, egressTime);
			
			return result.toString();
		}).returns(String.class);
		
		
		metrics.addSink(new FlinkKafkaProducer010<String>(
		        args[1],       
		        args[3],
		        new SimpleStringSchema()));
		
		env.execute("Flink Benchmark");
	}
}
