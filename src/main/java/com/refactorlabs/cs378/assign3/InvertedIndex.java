package com.refactorlabs.cs378.assign3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
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
import com.refactorlabs.cs378.assign2.LongArrayWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class InvertedIndex {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
//	public final static LongWritable ONE = new LongWritable(1L);
	public final static Writable[] writableValue = new Writable[1];
	public final static TextArrayWritable theValue = new TextArrayWritable();

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, TextArrayWritable> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		private Text valWord = new Text();
		private String theKey;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			Map<String, String> wordMap = Maps.newHashMap();
			
			if(tokenizer.hasMoreTokens()) 
				theKey = tokenizer.nextToken();
			
			// For each word in the input line, emit a count of 1 for that word.
			while (tokenizer.hasMoreTokens()) {
				String theToken = tokenizer.nextToken();
				//removing the punctuation
				theToken = theToken.replaceAll("--", " ");
				theToken = theToken.replaceAll("(?<=.)(?=\\[.+\\])", " ");
				theToken = theToken.replaceAll("[.,:;?!\"_]", " ");
				theToken = theToken.toLowerCase();
				theToken = theToken.trim();
				
				String[] strs = StringUtils.split(theToken);
				
				// count the number word appears for a paragraph
				for(String str : strs){
					str = str.replace(" ", "");
					if(!wordMap.containsKey(str)){
						wordMap.put(str, theKey);
					}	
				}
			}
			
			for(Map.Entry<String, String> entry : wordMap.entrySet()){
				word.set(entry.getKey());
				valWord.set(entry.getValue());
				writableValue[0] = valWord;
				TextArrayWritable output = new TextArrayWritable();
				output.set(writableValue);
				context.write(word, output);	
			}
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, TextArrayWritable, Text, TextArrayWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			
			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			
			SortedSet<Text> wordSet = new TreeSet<Text>();
			
			// Sum up the counts for the current word, specified in object "key".
			for (TextArrayWritable value : values) {
				for(int i = 0; i < value.get().length; i++){
					wordSet.add((Text)value.get()[i]);
				}
			}
			
			Text[] txts = wordSet.toArray(new Text[wordSet.size()]);

			TextArrayWritable output = new TextArrayWritable();
			output.set(txts);
			// Emit the total count for the word.
			context.write(key, output);
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

		Job job = new Job(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);        
		job.setMapOutputValueClass(TextArrayWritable.class); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextArrayWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(ReduceClass.class);
		
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
