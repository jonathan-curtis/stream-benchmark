package com.curtis.utils;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.api.java.JavaDStream;

import scala.Tuple2;

public class SparkBenchmarkingUtils {
	
	public static void emitMetrics(JavaDStream<Tuple2<?, Long>> lines, String host, String topic) {
		lines.foreachRDD(rdd -> {
			rdd.foreachPartition(iter -> {
				Map<String, Object> kafkaOutputParams = new HashMap<>();
				kafkaOutputParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
				kafkaOutputParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				kafkaOutputParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				kafkaOutputParams.put(ProducerConfig.ACKS_CONFIG, "0");
				KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaOutputParams);
				
				while (iter.hasNext()){
					Long stimulusTime = iter.next()._2;
					Long egressTime = Instant.now().toEpochMilli();
					List<Long> result = Arrays.asList(stimulusTime, egressTime);

					ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, result.toString());
					producer.send(producerRecord);
				}
				producer.close();
			});
		});
	}
}
