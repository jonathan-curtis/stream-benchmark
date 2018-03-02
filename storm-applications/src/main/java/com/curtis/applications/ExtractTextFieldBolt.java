package com.curtis.applications;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;

public class ExtractTextFieldBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -8348779644968544724L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("text", "timestamp"));
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {

		JSONObject jsonObject = new JSONObject(tuple.getStringByField("value"));
		String text;
		try {
			text = jsonObject.getString("text");
		} catch (JSONException jse) {
			// Not a Tweet object so setting text to blank
			text = "";
		}

		outputCollector.emit(new Values(text, tuple.getLongByField("timestamp")));
	}
}
