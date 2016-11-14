package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.sessions.*;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SessionFilter extends Configured implements Tool {
	public static class FilterMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

		private static final String DISCARD = "Discarded others";
		private static final String CATEGORY = "Category counts";
		private AvroMultipleOutputs multipleOutputs;
		private Random rands = new Random();
	    
		@Override
		public void setup(Context context){
			multipleOutputs = new AvroMultipleOutputs(context);
		}
		
	    @Override
	    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
	            throws IOException, InterruptedException {
	        
	        Session session = value.datum();
	    	
	    	List<Event> eventList = session.getEvents();
	    	SessionType stype = null;
	    	
	    	for(Event e : eventList){
	    		if(e.getEventType() == EventType.CHANGE || e.getEventType() == EventType.CONTACT_FORM_STATUS || e.getEventType() == EventType.EDIT || e.getEventType() == EventType.SUBMIT){
	    			stype = SessionType.SUBMITTER;
	    		}
	    		else if(e.getEventType() == EventType.ILMR_CPO){
	    			if(stype != SessionType.SUBMITTER){
	    				stype = SessionType.CPO;
	    			}
	    		}
	    		else if(e.getEventType() == EventType.CLICK || e.getEventType() == EventType.PLAY || e.getEventType() == EventType.PRINT){
	    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO){
		    			stype = SessionType.CLICKER;
	    			}
	    		}
	    		else if(e.getEventType() == EventType.SHOW){
	    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO && stype != SessionType.CLICKER){
	    				stype = SessionType.SHOWER;
	    			}
	    		}
	    		else if(e.getEventType() == EventType.VISIT){
	    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO && stype != SessionType.CLICKER && stype != SessionType.SHOWER){
	    				stype = SessionType.VISITOR;
	    			}
	    		}
	    		else{
	    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO && stype != SessionType.CLICKER && stype != SessionType.SHOWER && stype != SessionType.VISITOR){
		    			stype = SessionType.OTHER;
	    			}
	    		}	
	    	}

	    	if(stype != SessionType.OTHER || rands.nextDouble() < 0.001){
	    		multipleOutputs.write(stype.getText(), key, value);
	    		context.getCounter(CATEGORY, stype.getText()).increment(1L);
	    	}
	    	else{
	    		context.getCounter(DISCARD, "Discarded Others").increment(1L);
	    	}
	    }
	    public void cleanup(Context context) throws InterruptedException, IOException{
	    	 multipleOutputs.close(); 
	    }
	}

    	
	/**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
    	if (args.length != 2) {
			System.err.println("Usage: SessionFilter program <input path1> <output path>");
			return -1;
		}

		Configuration conf = getConf();
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "SessionFilter");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionFilter.class);
		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		
		// Mapper.
		job.setInputFormatClass(AvroKeyValueInputFormat.class);
		job.setMapperClass(FilterMapper.class);
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
		
		// Specify input key schema for avro input type.
		AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setInputValueSchema(job, Session.getClassSchema());
		
		// Specify output key schema for avro output type.
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Set avro multiple output related setting.
		AvroMultipleOutputs.setCountersEnabled(job, true);
		AvroMultipleOutputs.addNamedOutput(job, "submitter", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "cpo", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "clicker", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "shower", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "visitor", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		AvroMultipleOutputs.addNamedOutput(job, "other", AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
        int res = ToolRunner.run(new Configuration(), new SessionFilter(), args);
        System.exit(res);
    }
}
