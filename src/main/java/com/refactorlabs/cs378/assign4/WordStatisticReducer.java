package com.refactorlabs.cs378.assign4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordStatisticReducer extends Reducer<Text, WordStatisticWritable, Text, WordStatisticWritable> {
	/**
	 * Counter group for the reducer.  Individual counters are grouped for the reducer.
	 */
	private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

	@Override
	public void reduce(Text key, Iterable<WordStatisticWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long[] longVal = {0L, 0L, 0L};
		context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

		// Sum up the counts for the current word, specified in object "key".
		for (WordStatisticWritable value : values) {
			longVal[0] += (long)value.get()[0];
			longVal[1] += (long)value.get()[1];
			longVal[2] += (long)value.get()[2];
		}
		
		double mean = (double)longVal[1]/(double)longVal[0]; // mean
		double variance = ((double)mean * mean) * longVal[0] + (-2 * (double)mean * (double)longVal[1]) + (double)longVal[2];
		variance = variance / longVal[0]; // variance
		
		WordStatisticWritable WSW = new WordStatisticWritable(longVal[0], longVal[1], longVal[2], mean, variance);
		
		// Emit the output for the word.
		context.write(key, WSW);
	}
}
