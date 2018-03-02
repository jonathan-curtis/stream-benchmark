package com.curtis.benchmarking;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;


public class BenchmarkingApp {

	private static final int TEN_MINUTES = 600000;
	
	public static final int NUM_WORKERS = 3;

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		if(args.length != 5) {
			throw new IllegalArgumentException("4 arguments are required to execute a benchmark:\n" +
					"arg0 = benchmark to run - {Identity, Extraction, Grep, WordCount}\r\n" + 
					"arg1 = Kafka host name and port\r\n" + 
					"arg2 = input topic\r\n" + 
					"arg3 = output topic\r\n" + 
					"arg4 = app name");
		}
		
		Config config = new Config();
		config.setNumWorkers(NUM_WORKERS);
		config.setMessageTimeoutSecs(60);
		
		StormTopology topology = TopologyFactory.getBenchmarkTopology(args);
	
		StormSubmitter.submitTopology(args[4],
				config,
				topology);
	}
}
