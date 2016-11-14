package com.refactorlabs.cs378.assign7;
import com.google.common.collect.Maps;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


public class SessionJoinReducer extends Configured implements Tool {

	// <Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>>????
	
	
	
    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class SessionReducerClass extends Reducer<Text, AvroValue<VinImpressionCounts>,
            Text, AvroValue<VinImpressionCounts>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {
            
            context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
            
            long unique_user = 0L;
        	Map<CharSequence, Long> clicks = Maps.newHashMap();
        	long show_badge_detail = 0L;
        	long edit_contact_form = 0L;
        	long submit_contact_form = 0L;
        	long marketplace_srps = 0L;
        	long marketplace_vdps = 0L;
            
            for(AvroValue<VinImpressionCounts> value : values){
            	unique_user += value.datum().getUniqueUser();
        		
        		Map<CharSequence, Long> valClick = value.datum().getClicks();
        		if(valClick != null){
	        		for(CharSequence clickKey : valClick.keySet()){
	        			if(!clicks.containsKey(clickKey))
	        				clicks.put(clickKey, valClick.get(clickKey));
	        			else{
	        				long count = clicks.get(clickKey);
	        				clicks.put(clickKey, valClick.get(clickKey) + count);
	        			}
	        		}
        		}
        		
        		show_badge_detail += value.datum().getShowBadgeDetail();
        		edit_contact_form += value.datum().getEditContactForm();
        		submit_contact_form += value.datum().getSubmitContactForm();
        		if(marketplace_srps != 0)
        			marketplace_srps += value.datum().getMarketplaceSrps();
        		if(marketplace_vdps != 0)
        			marketplace_vdps += value.datum().getMarketplaceVdps();
            }
            if(unique_user != 0L){
            	VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            	builder.setUniqueUser(unique_user);
            	builder.setClicks(clicks);
            	builder.setShowBadgeDetail(show_badge_detail);
            	builder.setEditContactForm(edit_contact_form);
            	builder.setSubmitContactForm(submit_contact_form);
            	builder.setMarketplaceSrps(marketplace_srps);
            	builder.setMarketplaceVdps(marketplace_vdps);
            	
            	context.write(key, new AvroValue<VinImpressionCounts>(builder.build()));
            }
        }
    }
    
	
	 /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
//    	if (args.length != 2) {
//			System.err.println("Usage: WordCountB <input path> <output path>");
//			return -1;
//		}

		Configuration conf = getConf();

		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "SessionJoinReducer");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionJoinReducer.class);

		// Specify the Map
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		// Specify the Combiner
		job.setCombinerClass(JoinCombiner.class);
		
		// Specify the Reduce
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setReducerClass(SessionReducerClass.class);
		job.setOutputKeyClass(Text.class);
		AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

		// Grab the input file and output directory from the command line.
		MultipleInputs.addInputPath(job, new Path(args[0]), AvroKeyValueInputFormat.class, AvroMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CsvMapper.class);
//		String[] inputPaths = appArgs[0].split(",");
//		for ( String inputPath : inputPaths ) {
//			FileInputFormat.addInputPath(job, new Path(inputPath));
//		}
		FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new SessionJoinReducer(), args);
        System.exit(res);
    }

}
