package com.curtis.metrics;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class MetricsWriter {
	
	public static int count = 0;
	
	public static void main(final String[] args) throws Exception {
		
		if(args.length != 4) {
			throw new IllegalArgumentException("Application name, Kafka host(s), output topic name, and file output path are required");
		}
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, args[0]);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-reader");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put("processing.guarantee", "exactly_once");

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder.stream(args[2]);

		KStream<String, String> metrics = textLines.mapValues(value -> {
			String cleanedValue = value.replaceAll("[\\[\\]\\s+]", "");
			String[] arr = cleanedValue.split(",");
			long stimulusTime = Long.valueOf(arr[0]);
			long egressTime = Long.valueOf(arr[1]);

			long latency = egressTime - stimulusTime;

			return stimulusTime + "," + egressTime + "," + latency;
		});
		
		FileWriter writer = new FileWriter();
		writer.start(args[3]);
		metrics.foreach((x,y) -> {
			writer.write(y);
		});
		
		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.start();
	}
}


class FileWriter {

    private PrintStream outputStream;
	
	public void write(String value) {
		outputStream.println(value);
	}
	
	public void start(String filename) throws UnsupportedEncodingException, FileNotFoundException {
		 outputStream = new PrintStream(new FileOutputStream(filename, true), false,
                 StandardCharsets.UTF_8.name());
	}
	
    public void stop() {
        if (outputStream != null && outputStream != System.out)
            outputStream.close();
    }
}
