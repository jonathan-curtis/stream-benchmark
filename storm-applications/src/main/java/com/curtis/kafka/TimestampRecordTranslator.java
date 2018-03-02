package com.curtis.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TimestampRecordTranslator<K, V> implements RecordTranslator<K, V> {

	private static final long serialVersionUID = -854815544010309961L;
	
	public static final Fields FIELDS = new Fields("topic", "timestamp", "partition", "offset", "key", "value");
	
    @Override
    public List<Object> apply(ConsumerRecord<K, V> record) {
        return new Values(record.topic(),
        		record.timestamp(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        return FIELDS;
    }

    @Override
    public List<String> streams() {
        return DEFAULT_STREAM;
    }
}