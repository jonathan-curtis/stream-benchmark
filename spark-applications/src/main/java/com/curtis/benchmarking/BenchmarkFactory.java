package com.curtis.benchmarking;

import com.curtis.applications.Grep;
import com.curtis.applications.Identity;
import com.curtis.applications.Extraction;
import com.curtis.applications.WordCount;

public class BenchmarkFactory {

	@SuppressWarnings("rawtypes")
	public static Benchmark getBenchmark(String benchmarkType) {

		Benchmark benchmark = null;
		
		switch(benchmarkType) {
		case "Identity": benchmark = new Identity();
				break;
		case "Extraction": benchmark = new Extraction();
				break;
		case "Grep": benchmark = new Grep();
				break;
		case "WordCount": benchmark = new WordCount();
				break;
		default: throw new IllegalArgumentException("Benchmark " + benchmarkType + " not found");
		}
		
		return benchmark;
	}
}
