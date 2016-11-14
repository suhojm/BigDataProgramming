package com.refactorlabs.cs378.assign6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCount {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	public final static LongWritable ONE = new LongWritable(1L);

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

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

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
//			BufferedReader headerFile = new BufferedReader(new FileReader("dataSet6Header.tsv"));
//			String[] header = headerFile.readLine().split("\t");
			
//			headerFile.close();
			
			String line = value.toString();
			String[] tokens = line.split("\t");
			
			word.set("user_id:"+tokens[0]);
			context.write(word, ONE);
			word.set("event_type:"+tokens[1]);
			context.write(word, ONE);
			word.set("page:"+tokens[2]);
			context.write(word, ONE);
			word.set("referrer:"+tokens[3]);
			context.write(word, ONE);
			word.set("referring_domain:"+tokens[4]);
			context.write(word, ONE);
			word.set("event_timestamp:"+tokens[5]);
			context.write(word, ONE);
			word.set("city:"+tokens[6]);
			context.write(word, ONE);
			word.set("region:"+tokens[7]);
			context.write(word, ONE);
			word.set("vin:"+tokens[8]);
			context.write(word, ONE);
			word.set("vehicle_condition:"+tokens[9]);
			context.write(word, ONE);
			word.set("year:"+tokens[10]);
			context.write(word, ONE);
			word.set("make:"+tokens[11]);
			context.write(word, ONE);
			word.set("model:"+tokens[12]);
			context.write(word, ONE);
			word.set("trim:"+tokens[13]);
			context.write(word, ONE);
			word.set("body_style:"+tokens[14]);
			context.write(word, ONE);
			word.set("subtrim:"+tokens[15]);
			context.write(word, ONE);
			word.set("cab_style:"+tokens[16]);
			context.write(word, ONE);
			word.set("initial_price:"+tokens[17]);
			context.write(word, ONE);
			word.set("mileage:"+tokens[18]);
			context.write(word, ONE);
			word.set("mpg:"+tokens[19]);
			context.write(word, ONE);
			word.set("exterior_color:"+tokens[20]);
			context.write(word, ONE);
			word.set("interior_color:"+tokens[21]);
			context.write(word, ONE);
			word.set("engine_displacement:"+tokens[22]);
			context.write(word, ONE);
			word.set("engine:"+tokens[23]);
			context.write(word, ONE);
			word.set("transmission:"+tokens[24]);
			context.write(word, ONE);
			word.set("drive_type:"+tokens[25]);
			context.write(word, ONE);
			word.set("fuel:"+tokens[26]);
			context.write(word, ONE);
			word.set("image_count:"+tokens[27]);
			context.write(word, ONE);
			word.set("initial_carfax_free_report:"+tokens[28]);
			context.write(word, ONE);
			word.set("carfax_one_owner:"+tokens[29]);
			context.write(word, ONE);
			word.set("initial_cpo:"+tokens[30]);
			context.write(word, ONE);	
			word.set("features:"+tokens[31]);
			context.write(word, ONE);
			
			
//			for(int i = 0; i < header.length; i++){
//				word.set(header[i]+ ":" +tokens[i]);
//				context.write(word, ONE);
//			}
		
			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);	
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
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

		Job job = new Job(conf, "WordCount");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordCount.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

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
