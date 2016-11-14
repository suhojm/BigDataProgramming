package com.refactorlabs.cs378.assign4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatistic {

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticWritable> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		private static final int initialCount = 1;
		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		
		
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			Map<String, Integer> wordCountMap = Maps.newHashMap();
			
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			// For each word in the input line, emit a count of 1 for that word.
			while (tokenizer.hasMoreTokens()) {
				String theToken = tokenizer.nextToken();
				
				//removing the puctuations
				theToken = theToken.replaceAll("--", " ");
				theToken = theToken.replaceAll("(?<=.)(?=\\[.+\\])", " ");
				theToken = theToken.replaceAll("[.,:;?!\"_]", " ");
				if(!theToken.equals("The"))
					theToken = theToken.toLowerCase();
				theToken = theToken.trim();

				String[] strs = theToken.split("\\s+");
				
				// count the number word appears for a paragraph
				for(String str : strs){
					if(str != "" && str != " "){
						if(wordCountMap.containsKey(str)){
							int cnt = wordCountMap.get(str);
							wordCountMap.put(str, ++cnt);
						}
						else
							wordCountMap.put(str, initialCount);
					}
				}
			}
			for(Map.Entry<String, Integer> entry : wordCountMap.entrySet()){
				int cnt = entry.getValue().intValue();
				word.set(entry.getKey());
				WordStatisticWritable WSW = new WordStatisticWritable(1L, cnt, cnt*cnt, 0.0, 0.0);

				context.write(word, WSW);
			}
		}
	}
	
	public static class CombinerClass extends Reducer<Text, WordStatisticWritable, Text, WordStatisticWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<WordStatisticWritable> values, Context context)
				throws IOException, InterruptedException {
			
			long[] longVal = {0L,0L,0L};
		
			// combine those values for later use.
			for(WordStatisticWritable value : values){
				longVal[0] += (long)value.get()[0]; // number of paragraph where the key appeared
				longVal[1] += (long)value.get()[1]; // total number for word appeared so far
				longVal[2] += (long)value.get()[2]; // summing up the squared count
			}
			WordStatisticWritable WSW = new WordStatisticWritable(longVal[0], longVal[1], longVal[2], 0.0, 0.0);
			
			// Emit the total count for the word.
			context.write(key, WSW);
		}
	}
	

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticWritable, Text, WordStatisticWritable> {

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
			
			// Emit the total count for the word.
			context.write(key, WSW);
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "WordStatistic");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistic.class);

		
		job.setMapOutputKeyClass(Text.class);        
		job.setMapOutputValueClass(WordStatisticWritable.class); 
		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		// Using WordStatisticReducer for both Combiner and Reducer
		job.setReducerClass(WordStatisticReducer.class);
		job.setCombinerClass(WordStatisticReducer.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}
