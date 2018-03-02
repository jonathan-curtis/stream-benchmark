package com.curtis.benchmarking;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import com.curtis.applications.ExtractTextFieldBolt;
import com.curtis.applications.ExtractTimestampPrepareKafkaOutputBolt;
import com.curtis.applications.GrepBolt;
import com.curtis.applications.WordCountBolt;
import com.curtis.kafka.TimestampRecordTranslator;

public class TopologyFactory {

	public static final String GROUP_ID = "storm-benchmarking";
	
	public static final int NUM_EXECUTORS = 6;

	public static StormTopology getBenchmarkTopology(String...args) {

		String benchmarkType = args[0];

		TopologyBuilder builder = new TopologyBuilder();

		Properties kafkaParams = new Properties();
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		builder.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig
				.builder(args[1], args[2])
				.setProp(kafkaParams)
				.setRecordTranslator(new TimestampRecordTranslator<String, String>())
				.build()), 6);

		switch(benchmarkType) {
		case "Identity": createIdentityTopology(builder, args);
		break;
		case "Extraction": createExtractionTopology(builder, args);
		break;
		case "Grep": createGrepTopology(builder, args);
		break;
		case "WordCount": createWordCountTopology(builder, args);
		break;
		default: throw new IllegalArgumentException("Benchmark " + benchmarkType + " not found");
		}

		Config config = new Config();
		config.setDebug(true);

		return builder.createTopology();
	}


	public static void createIdentityTopology(TopologyBuilder builder, String...args) {
		builder
		.setBolt("extract_timestamp", new ExtractTimestampPrepareKafkaOutputBolt(), NUM_EXECUTORS)
		.shuffleGrouping("kafka_spout");

		builder.setBolt("forward_to_kafka", getKakfaBolt(args), NUM_EXECUTORS).shuffleGrouping("extract_timestamp");
	}

	public static void createExtractionTopology(TopologyBuilder builder, String...args) {
		builder
		.setBolt("extract_text", new ExtractTextFieldBolt(), NUM_EXECUTORS)
		.shuffleGrouping("kafka_spout");

		builder
		.setBolt("extract_timestamp", new ExtractTimestampPrepareKafkaOutputBolt(), NUM_EXECUTORS)
		.shuffleGrouping("extract_text");

		builder.setBolt("forwardToKafka", getKakfaBolt(args), 6).shuffleGrouping("extract_timestamp");
	}

	public static void createGrepTopology(TopologyBuilder builder, String...args) {
		builder
		.setBolt("extract_text", new ExtractTextFieldBolt(), NUM_EXECUTORS)
		.shuffleGrouping("kafka_spout");

		builder
		.setBolt("grep", new GrepBolt(), NUM_EXECUTORS)
		.shuffleGrouping("extract_text");

		builder
		.setBolt("extract_timestamp", new ExtractTimestampPrepareKafkaOutputBolt(), NUM_EXECUTORS)
		.shuffleGrouping("grep");

		builder.setBolt("forwardToKafka", getKakfaBolt(args), NUM_EXECUTORS).shuffleGrouping("extract_timestamp");
	}

	public static void createWordCountTopology(TopologyBuilder builder, String...args) {
		builder
		.setBolt("extract_text", new ExtractTextFieldBolt(), NUM_EXECUTORS)
		.shuffleGrouping("kafka_spout");

		builder
		.setBolt("word_count", new WordCountBolt(), NUM_EXECUTORS)
		.shuffleGrouping("extract_text");

		builder
		.setBolt("extract_timestamp", new ExtractTimestampPrepareKafkaOutputBolt(), NUM_EXECUTORS)
		.shuffleGrouping("word_count");

		builder.setBolt("forwardToKafka", getKakfaBolt(args), NUM_EXECUTORS).shuffleGrouping("extract_timestamp");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static KafkaBolt<String, String> getKakfaBolt(String[] args) {
		Properties kafkaOutputParams = new Properties();
		kafkaOutputParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[1]);
		kafkaOutputParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaOutputParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaOutputParams.put(ProducerConfig.ACKS_CONFIG, "0");

		return new KafkaBolt<String, String>().withProducerProperties(kafkaOutputParams)
				.withTopicSelector(new DefaultTopicSelector(args[3]))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
	}
}
