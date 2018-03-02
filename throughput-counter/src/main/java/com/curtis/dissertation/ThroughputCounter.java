package com.curtis.dissertation;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ThroughputCounter {

	public static final int SECOND = 1000; 
	public static final String TEMP = ".tmp";

	public static void main(String... args) throws IOException {

		if(args.length != 2) {
			throw new IllegalArgumentException("2 arguments are required to execute the throughput counter:\n" +
					"arg0 = input path\r\n" + 
					"arg1 = output path");
		}

		sortOutputBasedOnEgressTimestamp(args);
		calculateThroughputAndWriteResults(getEarliestStimulusTime(args), args);      
	}

	private static void calculateThroughputAndWriteResults(long earliestStimulusTime, String... args)
			throws IOException, UnsupportedEncodingException, FileNotFoundException {
		try (Reader reader = Files.newBufferedReader(Paths.get(args[0] + TEMP));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
			Iterable<CSVRecord> csvRecords = csvParser.getRecords();

			long timeBucket = 0;

			List<Tuple> listOfTuples = new ArrayList<>();

			int secondCounter = 1;

			for (CSVRecord csvRecord : csvRecords) {

				Long egressTime = Long.valueOf(csvRecord.get(1));

				if(timeBucket == 0) {
					timeBucket = earliestStimulusTime + SECOND;
					listOfTuples.add(new Tuple(timeBucket, 0, secondCounter));
				}

				if(egressTime < timeBucket) {		
					Tuple currentTuple = listOfTuples.get(listOfTuples.size() - 1);
					currentTuple.setCount(currentTuple.getCount() + 1);
					continue;
				} else if(egressTime >= timeBucket + SECOND) {

					while(egressTime >= timeBucket + SECOND) {
						timeBucket += 1000;
						secondCounter++;
						Tuple newTuple = new Tuple(timeBucket, 0, secondCounter);
						listOfTuples.add(newTuple);
					}

					timeBucket += SECOND;
					secondCounter++;
					Tuple newTuple = new Tuple(timeBucket, 1, secondCounter);
					listOfTuples.add(newTuple);
				} else if(egressTime >= timeBucket){
					timeBucket += SECOND;
					secondCounter++;
					Tuple newTuple = new Tuple(timeBucket, 1, secondCounter);
					listOfTuples.add(newTuple);
				}
			}

			FileWriter writer = new FileWriter();
			writer.start(args[1]);
			writer.write("timestamp" + "," + "second" + "," + "count");
			for(Tuple tuple1 : listOfTuples) {
				writer.write(tuple1.getTimestamp() + "," + tuple1.getSecond() + "," + tuple1.getCount());
			}

			writer.stop();
		}
	}

	private static void sortOutputBasedOnEgressTimestamp(String[] args) throws IOException {
		List<CSVRecord> listOfRecords = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(args[0]));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
			Iterable<CSVRecord> csvRecords = csvParser.getRecords();
			for (CSVRecord csvRecord : csvRecords) {
				listOfRecords.add(csvRecord);
			}		
			Collections.sort(listOfRecords, (CSVRecord x,CSVRecord y) ->  {
				long xTimestamp = Long.valueOf(x.get(1));
				long yTimestamp = Long.valueOf(y.get(1));

				long result = xTimestamp - yTimestamp;
				int valueToReturn = (int) result;
				return valueToReturn;
			});
		}

		FileWriter writer = new FileWriter();
		writer.start(args[0] + TEMP);

		for(CSVRecord csvRecord : listOfRecords) {
			writer.write(csvRecord.get(0) + "," + csvRecord.get(1) + "," + csvRecord.get(2));
		}

		writer.stop();
	}

	private static long getEarliestStimulusTime(String[] args) throws IOException {
		long earliestStimulusTime = 0L;

		try (Reader reader = Files.newBufferedReader(Paths.get(args[0]));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT)) {
			Iterable<CSVRecord> csvRecords = csvParser.getRecords();
			for (CSVRecord csvRecord : csvRecords) {

				long recordStimulusTime = Long.valueOf(csvRecord.get(0));

				if(earliestStimulusTime == 0L) {
					earliestStimulusTime = recordStimulusTime;
				}


				if(recordStimulusTime < earliestStimulusTime) {
					earliestStimulusTime = recordStimulusTime;
				}
			}
		}
		return earliestStimulusTime;
	}
}

class Tuple {
	private long timestamp;
	private long count;
	private long second;

	public Tuple() {

	}

	public Tuple(long timestamp, long count, long second) {
		this.timestamp = timestamp;
		this.count = count;
		this.second = second;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public long getSecond() {
		return second;
	}

	public void setSecond(long second) {
		this.second = second;
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
