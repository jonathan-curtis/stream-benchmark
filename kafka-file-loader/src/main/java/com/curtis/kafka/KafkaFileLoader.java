package com.curtis.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaFileLoader {

	public static boolean shouldThrottle = false;

	public static void main(String... args) throws IOException {

		if(!(args.length >= 3)) {
			throw new IllegalArgumentException("Kafka host(s), topic name, and file input path are "
					+ "required, with approximate throughput rate being an optional parameter");
		}

		int throughput = 1;

		if(args.length == 4) {
			int tmp = Integer.valueOf(args[3]);
			if (tmp > 0) {
				throughput = tmp;
				shouldThrottle = true;
			}
		}

		int waitPeriodNanoSeconds = 1_000_000_000 / throughput;

		Map<String, Object> kafkaOutputParams = new HashMap<>();
		kafkaOutputParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
		kafkaOutputParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaOutputParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaOutputParams);
		String topic = args[1];

		System.out.println("Beginning processing file: " + args[2]);
		
		try (Stream<String> stream = Files.lines(Paths.get(args[2]))) {
			stream.forEach(x -> {
				waitInNanoSeconds(waitPeriodNanoSeconds);
				ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, x);
				producer.send(producerRecord);
			});
		}

		System.out.println("Finished processing file: " + args[2]);
		producer.close();
	}

	public static void waitInNanoSeconds(long waitPeriodNanoSeconds){
		if(shouldThrottle) {
			long waitUntil = System.nanoTime() + waitPeriodNanoSeconds;
			while(waitUntil > System.nanoTime()){
				//Do nothing
			}
		}
	}
}

