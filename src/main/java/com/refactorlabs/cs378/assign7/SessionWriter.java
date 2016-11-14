package com.refactorlabs.cs378.assign7;
import com.refactorlabs.cs378.assign5.Utils;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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
import java.util.StringTokenizer;


public class SessionWriter extends Configured implements Tool {

	/**
	 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
	 */
	public static class SessionMapperClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {
	
	    /**
	     * Local variable "word" will contain the word identified in the input.
	     */
	    private Text word = new Text();
	    @Override
	    public void map(LongWritable key, Text value, Context context)
	            throws IOException, InterruptedException {
	        
	
	        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
	
	        String line = value.toString();
			String[] tokens = line.split("\t");
			
			
            Session.Builder builder = Session.newBuilder();
            
            //key
            word.set(tokens[0]);
            
            builder.setUserId(tokens[0]);
            List<Event> eventList = new ArrayList<Event>();
            Event event = new Event();
            // event.setEventType(EventType.CHANGE);
            // event.setEventSubtype(EventSubtype.ALTERNATIVE);
            separateEventType(tokens[1], event);
            if(tokens[5].equals("null") || tokens[5].equals(""))
            	event.setEventTime(null);
            else
            	event.setEventTime(tokens[5]);
          
            if(tokens[2].equals("null") || tokens[2].equals(""))
            	event.setPage(null);
            else
            	event.setPage(tokens[2]);
            
            if(tokens[3].equals("null") || tokens[3].equals(""))
            	event.setReferrer(null);
            else
            	event.setReferrer(tokens[3]);
            
            if(tokens[4].equals("null") || tokens[4].equals(""))
            	event.setReferringDomain(null);
            else
            	event.setReferringDomain(tokens[4]);
            
            if(tokens[6].equals("null") || tokens[6].equals(""))
            	event.setCity(null);
            else
            	event.setCity(tokens[6]);
            
            if(tokens[7].equals("null") || tokens[7].equals(""))
            	event.setRegion(null);
            else
            	event.setRegion(tokens[7]);
            
            if(tokens[8].equals("null") || tokens[8].equals(""))
            	event.setVin(null);
            else
            	event.setVin(tokens[8]);

            if(tokens[9].toUpperCase().equals("USED"))
            	event.setCondition(VehicleCondition.Used);
            else if(tokens[9].toUpperCase().equals("CPO"))
            	event.setCondition(VehicleCondition.CPO);
            else
            	event.setCondition(null);
            
            if(tokens[10].equals("null") || tokens[10].equals(""))
            	event.setYear(null);
            else
            	event.setYear(Integer.parseInt(tokens[10]));
            
            if(tokens[11].equals("null") || tokens[11].equals(""))
            	event.setMake(null);
            else
            	event.setMake(tokens[11]);
        
            if(tokens[12].equals("null") || tokens[12].equals(""))
            	event.setModel(null);
            else
            	event.setModel(tokens[12]);
            
            if(tokens[13].equals("null") || tokens[13].equals(""))
            	event.setTrim(null);
            else
            	event.setTrim(tokens[13]);

			if(tokens[14].toUpperCase().equals("COUPE"))
				event.setBodyStyle(BodyStyle.Coupe);
			else if (tokens[14].toUpperCase().equals("HATCHBACK")) 
				event.setBodyStyle(BodyStyle.Hatchback);	
			else if (tokens[14].toUpperCase().equals("MINIVAN")) 
				event.setBodyStyle(BodyStyle.Minivan);	
			else if (tokens[14].toUpperCase().equals("PICKUP")) 
				event.setBodyStyle(BodyStyle.Pickup);	
			else if (tokens[14].toUpperCase().equals("SUV")) 
				event.setBodyStyle(BodyStyle.SUV);	
			else if (tokens[14].toUpperCase().equals("SEDAN")) 
				event.setBodyStyle(BodyStyle.Sedan);
			else
				event.setBodyStyle(null);
			
			if(tokens[15].equals("null") || tokens[15].equals(""))
            	event.setSubtrim(null);
            else
            	event.setSubtrim(tokens[15]);

            if(tokens[16].toUpperCase().equals("Crew Cab"))
            	event.setCabStyle(CabStyle.CREW_CAB);
            else if(tokens[16].toUpperCase().equals("Regular Cab"))
            	event.setCabStyle(CabStyle.REGULAR_CAB);
            else
            	event.setCabStyle(null);
            
            if(tokens[17].equals("null") || tokens[17].equals(""))
            	event.setPrice(null);
            else
            	event.setPrice(Double.parseDouble(tokens[17]));
            
            if(tokens[18].equals("null") || tokens[18].equals(""))
            	event.setMileage(null);
            else
            	event.setMileage(Integer.parseInt(tokens[18]));
            
            if(tokens[19].equals("null") || tokens[19].equals(""))
            	event.setMpg(null);
            else
            	event.setMpg(Integer.parseInt(tokens[19]));
            
            if(tokens[20].equals("null") || tokens[20].equals(""))
            	event.setExteriorColor(null);
            else
            	event.setExteriorColor(tokens[20]);
            
            if(tokens[21].equals("null") || tokens[21].equals(""))
            	event.setInteriorColor(null);
            else
            	event.setInteriorColor(tokens[21]);
            
            if(tokens[22].equals("null") || tokens[22].equals(""))
            	event.setEngineDisplacement(null);
            else
            	event.setEngineDisplacement(tokens[22]);
            
            if(tokens[23].equals("null") || tokens[23].equals(""))
            	event.setEngine(null);
            else
            	event.setEngine(tokens[23]);
            
            if(tokens[24].equals("null") || tokens[24].equals(""))
            	event.setTransmission(null);
            else
            	event.setTransmission(tokens[24]);
            
            if(tokens[25].equals("null") || tokens[25].equals(""))
            	event.setDriveType(null);
            else
            	event.setDriveType(tokens[25]);
            
            if(tokens[26].equals("null") || tokens[26].equals(""))
            	event.setFuel(null);
            else
            	event.setFuel(tokens[26]);
            
            if(tokens[27].equals("null") || tokens[27].equals(""))
            	event.setImageCount(null);
            else
            	event.setImageCount(Integer.parseInt(tokens[27]));
            
            if(tokens[28].toLowerCase().equals("t"))
            	event.setFreeCarfaxReport(true);
            else
            	event.setFreeCarfaxReport(false);
            	
			if(tokens[29].toLowerCase().equals("t"))
            	event.setCarfaxOneOwner(true);
            else
            	event.setCarfaxOneOwner(false);

            if(tokens[30].toLowerCase().equals("t"))
            	event.setCpo(true);
            else
            	event.setCpo(false);

            String[] features = tokens[31].split(":");
            if(features.length == 1 && (features[0].equals("null") || tokens[0].equals("")))
            	event.setFeatures(null);
            else{
	            List<String> featureList = new ArrayList<String>();

	            for(int i = 0; i < features.length; i++){
	            	featureList.add(features[i]);
	            }
	            Collections.sort(featureList);
	            
	            List<CharSequence> featureListCS = new ArrayList<CharSequence>(featureList);
	            event.setFeatures(featureListCS);
            }

            eventList.add(event);
            
            builder.setEvents(eventList);
            
            context.write(word, new AvroValue<Session>(builder.build()));	       
	    }
	}

	public static void separateEventType(String e, Event event){
		if(e.equals("change contact form type")){
			event.setEventType(EventType.CHANGE);
			event.setEventSubtype(EventSubtype.CONTACT_FORM_TYPE);
		} else if(e.equals("click alternative")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.ALTERNATIVE);
		} else if(e.equals("click contact banner")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.CONTACT_BANNER);
		} else if(e.equals("click contact button")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.CONTACT_BUTTON);
		} else if(e.equals("click dealer phone")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.DEALER_PHONE);
		} else if(e.equals("click features section")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.FEATURES_SECTION);
		} else if(e.equals("click get directions")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.GET_DIRECTIONS);
		} else if(e.equals("click vehicle history report link")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
		} else if(e.equals("click_on_alternative")){
			event.setEventType(EventType.CLICK);
			event.setEventSubtype(EventSubtype.ALTERNATIVE);
		} else if(e.equals("contact form error")){
			event.setEventType(EventType.CONTACT_FORM_STATUS);
			event.setEventSubtype(EventSubtype.ERROR);
		} else if(e.equals("contact form success")){
			event.setEventType(EventType.CONTACT_FORM_STATUS);
			event.setEventSubtype(EventSubtype.SUCCESS);
		} else if(e.equals("alternatives_displayed")){
			event.setEventType(EventType.DISPLAY);
			event.setEventSubtype(EventSubtype.ALTERNATIVES);
		} else if(e.equals("display_ilmr_report_listing")){
			event.setEventType(EventType.DISPLAY);
			event.setEventSubtype(EventSubtype.ILMR_REPORT_LISTING);
		} else if(e.equals("edit contact form")){
			event.setEventType(EventType.EDIT);
			event.setEventSubtype(EventSubtype.CONTACT_FORM);
		} else if(e.equals("error_loading_ilmr")){
			event.setEventType(EventType.ILMR_STATUS);
			event.setEventSubtype(EventSubtype.LOAD_ERROR);
		} else if(e.equals("ilmr_cpo_financing")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.FINANCING);
		} else if(e.equals("ilmr_cpo_inspection")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.INSPECTION);
		} else if(e.equals("ilmr_cpo_roadside")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.ROADSIDE);
		} else if(e.equals("ilmr_cpo_sirius")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.SIRIUS);
		} else if(e.equals("ilmr_cpo_warranty")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.WARRANTY);
		} else if(e.equals("ilmr_see_more_cpo")){
			event.setEventType(EventType.ILMR_CPO);
			event.setEventSubtype(EventSubtype.SEE_MORE);
		} else if(e.equals("play_video_ilmr")){
			event.setEventType(EventType.PLAY);
			event.setEventSubtype(EventSubtype.ILMR_VIDEO);
		} else if(e.equals("print_ilmr")){
			event.setEventType(EventType.PRINT);
			event.setEventSubtype(EventSubtype.ILMR);
		} else if(e.equals("show badge detail")){
			event.setEventType(EventType.SHOW);
			event.setEventSubtype(EventSubtype.BADGE_DETAIL);
		} else if(e.equals("show photo modal")){
			event.setEventType(EventType.SHOW);
			event.setEventSubtype(EventSubtype.PHOTO_MODAL);
		} else if(e.equals("submit contact form")){
			event.setEventType(EventType.SUBMIT);
			event.setEventSubtype(EventSubtype.CONTACT_FORM);
		} else if(e.equals("visit alternatives")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.ALTERNATIVES);
		} else if(e.equals("visit badges")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.BADGES);
		} else if(e.equals("visit contact form")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.CONTACT_FORM);
		} else if(e.equals("visit features")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.FEATURES);
		} else if(e.equals("visit vehicle history")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.VEHICLE_HISTORY);
		} else if(e.equals("visit_market_report_listing")){
			event.setEventType(EventType.VISIT);
			event.setEventSubtype(EventSubtype.MARKET_REPORT_LISTING);
		} else{
			event.setEventType(null);
			event.setEventSubtype(null);
		}
		
		
	}
		
	
    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class SessionReducerClass extends Reducer<Text, AvroValue<Session>,
            AvroKey<CharSequence>, AvroValue<Session>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {
            
            context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
            
            List<Event> eventList = new ArrayList<Event>();
            
            for (AvroValue<Session> value : values) {
                List<Event> valueEvent = value.datum().getEvents();
                for(Event e : valueEvent)
                	eventList.add(e);
            }
            
            Collections.sort(eventList, new Comparator<Event>(){

				@Override
				public int compare(Event e1, Event e2) {
					String[] e1time = e1.getEventTime().toString().replace(":", " ").replace("-", " ").split("\\s+");
					String[] e2time = e2.getEventTime().toString().replace(":", " ").replace("-", " ").split("\\s+");
					if(!e1time[0].equals(e2time[0])){
						//year
						return Integer.parseInt(e1time[0])-Integer.parseInt(e2time[0]);
					}
					else if(!e1time[1].equals(e2time[1])){
						//month
						return Integer.parseInt(e1time[1])-Integer.parseInt(e2time[1]);
					}
					else if(!e1time[2].equals(e2time[2])){
						//day
						return Integer.parseInt(e1time[2])-Integer.parseInt(e2time[2]);
					}
					else if(!e1time[3].equals(e2time[3])){
						//hour
						return Integer.parseInt(e1time[3])-Integer.parseInt(e2time[3]);
					}
					else if(!e1time[4].equals(e2time[4])){
						//minute
						return Integer.parseInt(e1time[4])-Integer.parseInt(e2time[4]);
					}
					else if(!e1time[5].replace(".", "").equals(e2time[5].replace(".",""))){
						//seconds
						return Integer.parseInt(e1time[5].replace(".", ""))-Integer.parseInt(e2time[5].replace(".", ""));
					}
					else
						return 0;
				}
            });
      
            // Emit the total count for the word.
            Session.Builder builder = Session.newBuilder();
            builder.setUserId(key.toString());
            builder.setEvents(eventList);
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(builder.build()));
        }
    }
    
	
	 /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
    	if (args.length != 2) {
			System.err.println("Usage: WordCountB <input path> <output path>");
			return -1;
		}

		Configuration conf = getConf();

		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "SessionWriter");
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(SessionWriter.class);

		// Specify the Map
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SessionMapperClass.class);
		job.setMapOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

		// Specify the Reduce
		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(SessionReducerClass.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Session.getClassSchema());

		// Grab the input file and output directory from the command line.
		String[] inputPaths = appArgs[0].split(",");
		for ( String inputPath : inputPaths ) {
			FileInputFormat.addInputPath(job, new Path(inputPath));
		}
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
        int res = ToolRunner.run(new Configuration(), new SessionWriter(), args);
        System.exit(res);
    }

}
