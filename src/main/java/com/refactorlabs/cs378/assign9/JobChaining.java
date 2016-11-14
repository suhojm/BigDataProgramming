package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
//import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import com.refactorlabs.cs378.sessions.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.refactorlabs.cs378.assign8.*;
import com.refactorlabs.cs378.assign8.SessionFilter.FilterMapper;

public class JobChaining extends Configured implements Tool {
	
	// Mapper for submitter

	public static class SubmitterMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>>{
		@Override
	    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
	            throws IOException, InterruptedException {
			
	        Map<EventSubtype, Integer> eventMap = Maps.newHashMap();
	        Session session = value.datum();
	        List<Event> eventList = session.getEvents();
	        
	        for(Event e : eventList){
	        	if(e.getEventType() == EventType.CLICK){
	        		if(eventMap.containsKey(e.getEventSubtype())){
	        			int cnt = eventMap.get(e.getEventSubtype());
	        			eventMap.put(e.getEventSubtype(), ++cnt);
	        		}
	        		else{
	        			eventMap.put(e.getEventSubtype(), Utils.initialCount);
	        		}
	        	}
	        }
	        
	        for(EventSubtype e : EventSubtype.values()){
	        	ClickSubtypeStatisticsKey.Builder kBuilder = ClickSubtypeStatisticsKey.newBuilder();
	        	kBuilder.setSessionType("Submitter");
	        	kBuilder.setClickSubtype(e.name());
	        	
	        	ClickSubtypeStatisticsData.Builder dBuilder = ClickSubtypeStatisticsData.newBuilder();
	        	dBuilder.setSessionCount(Utils.initialCount);
	        	if(eventMap.containsKey(e)){
	        		int cnt = eventMap.get(e);
	        		dBuilder.setTotalCount(cnt);
	        		dBuilder.setSumOfSquares((long)cnt*(long)cnt);
	        	}
	        	else{
	        		dBuilder.setTotalCount(0);
	        		dBuilder.setSumOfSquares(0);
	        	}
	        	
	        	context.write(new AvroKey<ClickSubtypeStatisticsKey>(kBuilder.build()), new AvroValue<ClickSubtypeStatisticsData>(dBuilder.build()));
	        }
	    }
	}	
	
		
		
	// Mapper for cpo
	
	public static class CpoMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>>{
		@Override
	    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
	            throws IOException, InterruptedException {
			
	        Map<EventSubtype, Integer> eventMap = Maps.newHashMap();
	        Session session = value.datum();
	        List<Event> eventList = session.getEvents();
	        
	        for(Event e : eventList){
	        	if(e.getEventType() == EventType.CLICK){
	        		if(eventMap.containsKey(e.getEventSubtype())){
	        			int cnt = eventMap.get(e.getEventSubtype());
	        			eventMap.put(e.getEventSubtype(), ++cnt);
	        		}
	        		else{
	        			eventMap.put(e.getEventSubtype(), Utils.initialCount);
	        		}
	        	}
	        }
	        
	        for(EventSubtype e : EventSubtype.values()){
	        	ClickSubtypeStatisticsKey.Builder kBuilder = ClickSubtypeStatisticsKey.newBuilder();
	        	kBuilder.setSessionType("CPO");
	        	kBuilder.setClickSubtype(e.name());
	        	
	        	ClickSubtypeStatisticsData.Builder dBuilder = ClickSubtypeStatisticsData.newBuilder();
	        	dBuilder.setSessionCount(Utils.initialCount);
	        	if(eventMap.containsKey(e)){
	        		int cnt = eventMap.get(e);
	        		dBuilder.setTotalCount(cnt);
	        		dBuilder.setSumOfSquares((long)cnt*(long)cnt);
	        	}
	        	else{
	        		dBuilder.setTotalCount(0);
	        		dBuilder.setSumOfSquares(0);
	        	}
	        	
	        	context.write(new AvroKey<ClickSubtypeStatisticsKey>(kBuilder.build()), new AvroValue<ClickSubtypeStatisticsData>(dBuilder.build()));
	        }
	    }
	}
	
	
	
	// Mapper for clicker
	
	public static class ClickerMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>>{
		@Override
	    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
	            throws IOException, InterruptedException {
			
	        Map<EventSubtype, Integer> eventMap = Maps.newHashMap();
	        Session session = value.datum();
	        List<Event> eventList = session.getEvents();
	        
	        for(Event e : eventList){
	        	if(e.getEventType() == EventType.CLICK){
	        		if(eventMap.containsKey(e.getEventSubtype())){
	        			int cnt = eventMap.get(e.getEventSubtype());
	        			eventMap.put(e.getEventSubtype(), ++cnt);
	        		}
	        		else{
	        			eventMap.put(e.getEventSubtype(), Utils.initialCount);
	        		}
	        	}
	        }
	        
	        for(EventSubtype e : EventSubtype.values()){
	        	ClickSubtypeStatisticsKey.Builder kBuilder = ClickSubtypeStatisticsKey.newBuilder();
	        	kBuilder.setSessionType("Clicker");
	        	kBuilder.setClickSubtype(e.name());
	        	
	        	ClickSubtypeStatisticsData.Builder dBuilder = ClickSubtypeStatisticsData.newBuilder();
	        	dBuilder.setSessionCount(Utils.initialCount);
	        	if(eventMap.containsKey(e)){
	        		int cnt = eventMap.get(e);
	        		dBuilder.setTotalCount(cnt);
	        		dBuilder.setSumOfSquares((long)cnt*(long)cnt);
	        	}
	        	else{
	        		dBuilder.setTotalCount(0);
	        		dBuilder.setSumOfSquares(0);
	        	}
	        	
	        	context.write(new AvroKey<ClickSubtypeStatisticsKey>(kBuilder.build()), new AvroValue<ClickSubtypeStatisticsData>(dBuilder.build()));
	        }
	    }		
	}
		
	
	
	// Reducer
	
	public static class JobChainReducer extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>>{
		@Override
        public void reduce(AvroKey<ClickSubtypeStatisticsKey> key, Iterable<AvroValue<ClickSubtypeStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

            long sesCnt = 0L;
            long totCnt = 0L;
            long sumSqr = 0L;
            
            ClickSubtypeStatisticsKey theKey = key.datum();
            boolean subtypeAny = false;
            
            Set<Long> anyVals = Sets.newHashSet();
            // Sum up the counts for the current word, specified in object "key".
            for (AvroValue<ClickSubtypeStatisticsData> value : values) {
            	long cnt = value.datum().getSessionCount();
            	if(!subtypeAny)
            		sesCnt += cnt;
                totCnt += value.datum().getTotalCount();
                sumSqr += value.datum().getSumOfSquares();
                
                if(theKey.getClickSubtype().equals("any")){
                	subtypeAny = true;
                	sesCnt = cnt;
                	anyVals.add(cnt);
                }
            }
            if(theKey.getSessionType().equals("any") && theKey.getClickSubtype().equals("any")){
            	sesCnt = 0;
            	for(long lval : anyVals){
            		sesCnt += lval;
            	}
            }
            	     
            double mean = (double)totCnt / (double)sesCnt; // mean
            double variance = ((double)mean*mean) * sesCnt + (-2 * (double)mean * (double)totCnt) + (double)sumSqr;
            variance = variance / sesCnt; // variance
            
            // Emit the total count for the word.
            ClickSubtypeStatisticsData.Builder builder = ClickSubtypeStatisticsData.newBuilder();
            builder.setSessionCount(sesCnt);
            builder.setTotalCount(totCnt);
            builder.setSumOfSquares(sumSqr);
            builder.setMean(mean);
            builder.setVariance(variance);
            
            context.write(key, new AvroValue<ClickSubtypeStatisticsData>(builder.build()));            
        }
	}
	
	
	public static class JobAggregator extends Mapper<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>, AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>>{
		@Override
		public void map(AvroKey<ClickSubtypeStatisticsKey> key, AvroValue<ClickSubtypeStatisticsData> value, Context context)
				throws IOException, InterruptedException {
		
				context.write(key, value);
				
				ClickSubtypeStatisticsKey keyObj = key.datum();
				
				ClickSubtypeStatisticsKey.Builder builder = ClickSubtypeStatisticsKey.newBuilder();
				builder.setSessionType(keyObj.getSessionType());
				builder.setClickSubtype("any");
				context.write(new AvroKey(builder.build()), value);
				
				builder = ClickSubtypeStatisticsKey.newBuilder();
				builder.setSessionType("any");
				builder.setClickSubtype(keyObj.getClickSubtype());
				context.write(new AvroKey(builder.build()), value);
			
				builder = ClickSubtypeStatisticsKey.newBuilder();
				builder.setSessionType("any");
				builder.setClickSubtype("any");
				context.write(new AvroKey(builder.build()), value);
		}
	}
	


	/**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: JobChaining program <input path> <output path>");
            return -1;
        }

		Configuration conf = getConf();
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job a8job = Job.getInstance(conf, "SessionFilter");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		a8job.setJarByClass(JobChaining.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		
		// Mapper.
		a8job.setInputFormatClass(AvroKeyValueInputFormat.class);
		a8job.setMapperClass(FilterMapper.class);
		AvroJob.setMapOutputKeySchema(a8job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(a8job, Session.getClassSchema());
		
		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(a8job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(a8job, Session.getClassSchema());
		
		// Specify output key schema for avro output type.
		AvroJob.setOutputKeySchema(a8job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(a8job, Session.getClassSchema());

		// Set avro multiple output related setting.
		AvroMultipleOutputs.setCountersEnabled(a8job, true);
		AvroMultipleOutputs.addNamedOutput(a8job, "submitter", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(a8job, "cpo", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(a8job, "clicker", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(a8job, "shower", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(a8job, "visitor", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(a8job, "other", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		
		a8job.setNumReduceTasks(0);
		a8job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		
		FileInputFormat.addInputPath(a8job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(a8job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		a8job.waitForCompletion(true);

		
		// get outputs from assign8 as input for assign9
		Path path = new Path(appArgs[1]);
		FileSystem fs = path.getFileSystem(conf);
		
		//s3://utcs378/jk36326/output/("name of output folder") ....
		System.err.println("\nThe Path: " + path.toString() + "\n");
		
		FileStatus[] listStatus = fs.listStatus(path);
		List<String> assign8outputs = Lists.newArrayList();
		
		for (FileStatus fileStatus : listStatus) {
			assign8outputs.add(fileStatus.getPath().toString());
		}
		
        /*
         * Create three jobs Submitter, Cpo and Clicker
         */
        String[] mapperNames = new String[]{"SubmitterMapper", "CpoMapper", "ClickerMapper"};
        List<?> mapperClasses = Lists.newArrayList(SubmitterMapper.class, CpoMapper.class, ClickerMapper.class);
        String[] mapperRegexs = new String[]{".+submitter-m-.+\\.avro", ".+cpo-m-.+\\.avro", ".+clicker-m-.+\\.avro"};
        List<Job> jobs = Lists.newArrayList();
        List<String> outputPaths = Lists.newArrayList();
        
        int i = 0;
        for (String mapperName : mapperNames) {
            // Create job based on conf and mapperName
            Job job = Job.getInstance(conf, mapperName);
            
            // Identify the JAR file to replicate to all machines.
    		job.setJarByClass(JobChaining.class);
    		
    		// Specify input key schema for Avro input type.
    		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
    		AvroJob.setInputValueSchema(job, Session.getClassSchema());
    		
    		// Set the mapper input and the set the map class for it
    		job.setInputFormatClass(AvroKeyValueInputFormat.class);
    		
    		job.setMapperClass((Class<? extends Mapper>) mapperClasses.get(i));
    		
    		// Set the mapper output key and value
    		AvroJob.setMapOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
    		AvroJob.setMapOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());
    		
    		// Specify the Reducer
    		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    		job.setReducerClass(JobChainReducer.class);
    		
    		// Set the output key and value
    		AvroJob.setOutputKeySchema(job, ClickSubtypeStatisticsKey.getClassSchema());
    		AvroJob.setOutputValueSchema(job, ClickSubtypeStatisticsData.getClassSchema());
    		
    		// Obtain the correspond avro and configure the path
    		List<String> inputPaths = Lists.newArrayList();
    		Pattern pat = Pattern.compile(mapperRegexs[i]);
    		for(String strPath : assign8outputs){
    			Matcher m = pat.matcher(strPath);
    			if(m.matches())
    				inputPaths.add(strPath);
    		}
    		
    		
    		for ( String inputPath : inputPaths ) {
    			FileInputFormat.addInputPath(job, new Path(inputPath));
    		}
    		
    		// config output path
    		String oPath = appArgs[1] + Path.SEPARATOR + mapperName;
    		
    		// create output path
    		fs.create(new Path(oPath));
    		FileOutputFormat.setOutputPath(job, new Path(oPath));
    		
    		// save the output for later use for aggregate job
    		outputPaths.add(oPath);
    		
    		jobs.add(job);
    		i++;
        }

		// All jobs is submitted now
        jobs.get(0).submit();
        jobs.get(1).submit();
        jobs.get(2).submit();
		
		
		// To ensure every job is going to complete
		while(! (jobs.get(0).isComplete() && jobs.get(1).isComplete() && jobs.get(2).isComplete()) ) {
			Thread.sleep(3000);
		}
        
		// Do the Aggregate Job
		Job aggregateJob = Job.getInstance(conf, "JobAggregator");
		aggregateJob.setJarByClass(JobChaining.class);

		// Set input 
		AvroJob.setInputKeySchema(aggregateJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setInputValueSchema(aggregateJob, ClickSubtypeStatisticsData.getClassSchema());
		
		aggregateJob.setInputFormatClass(AvroKeyValueInputFormat.class);
		aggregateJob.setMapperClass(JobAggregator.class);
		
		// set map output
		AvroJob.setMapOutputKeySchema(aggregateJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setMapOutputValueSchema(aggregateJob, ClickSubtypeStatisticsData.getClassSchema());
		
		// set reducer and overall output
		aggregateJob.setOutputFormatClass(TextOutputFormat.class);
		aggregateJob.setReducerClass(JobChainReducer.class);
		
		AvroJob.setOutputKeySchema(aggregateJob, ClickSubtypeStatisticsKey.getClassSchema());
		AvroJob.setOutputValueSchema(aggregateJob, ClickSubtypeStatisticsData.getClassSchema());
		
		// get input and dir for output
		for (String outputPath : outputPaths) {
			FileInputFormat.addInputPath(aggregateJob, new Path(outputPath + Path.SEPARATOR + "part-r-00000.avro"));
		}
		
		String finalPath = appArgs[1] + Path.SEPARATOR + "final";
		fs.create(new Path(finalPath));
		FileOutputFormat.setOutputPath(aggregateJob, new Path(finalPath));
		
		// Wait 
		aggregateJob.waitForCompletion(true);
		
		return 0;	
    }
	
	
	
    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new JobChaining(), args);
        System.exit(res);
    }
}
