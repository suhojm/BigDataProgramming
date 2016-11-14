package com.refactorlabs.cs378.assign7;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

class SessionValue{
	
	Set<String> users;
	Map<CharSequence, Long> clicks;
	long show_badge_detail;
	long edit_contact_form;
	long submit_contact_form;
	
	public SessionValue(){
		users = Sets.newHashSet();
		clicks = Maps.newHashMap();
		show_badge_detail = 0;
		edit_contact_form = 0;
		submit_contact_form = 0;
	}
};


// THIS FIRST MAPPER GETS THE AVRO CONTAINER

// InputFormat : AvroKeyValueInputFormat
// AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>



/**
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class AvroMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
            throws IOException, InterruptedException {
        
        Session session = value.datum();
    	
    	context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

    	
    	
    	
//    	// ArrayList have UserId(String) and Event(Event)
    	Map<String, SessionValue> sessionMap = Maps.newHashMap();
		
    	List<Event> eventList = session.getEvents();
    	for(Event e : eventList){
    		String vin = e.getVin().toString();
    		
    		SessionValue val;
    		if(!sessionMap.containsKey(vin))
    			val = new SessionValue();
    		else
    			val = sessionMap.get(vin);	

    		boolean contained = false;
			if(val.users.contains(session.getUserId()))
				contained = true;
			
			val.users.add(session.getUserId().toString());
			
			if(e.getEventType() == EventType.SHOW && e.getEventSubtype() == EventSubtype.BADGE_DETAIL){
				if(!(contained && val.show_badge_detail != 0))
					val.show_badge_detail++;
            }
        	else if(e.getEventType() == EventType.EDIT && e.getEventSubtype() == EventSubtype.CONTACT_FORM){
        		if(!(contained && val.edit_contact_form != 0))
        			val.edit_contact_form++;
        	}
        	else if(e.getEventType() == EventType.SUBMIT && e.getEventSubtype() == EventSubtype.CONTACT_FORM){
        		if(!(contained && val.submit_contact_form != 0))
        			val.submit_contact_form++;
        	}
        	else if(e.getEventType() == EventType.CLICK){
        		if(!val.clicks.containsKey(e.getEventSubtype())){
        			val.clicks.put(e.getEventSubtype().name(), 1L);
        		}
        		else{
        			long count = val.clicks.get(e.getEventSubtype());
        			val.clicks.put(e.getEventSubtype().name(), count++);
        		}	
        	}
			sessionMap.put(vin, val);  
    	}
    	
    	for(Map.Entry<String, SessionValue> entry : sessionMap.entrySet()){
			word.set(entry.getKey());
			SessionValue val = entry.getValue();
			
			VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
			builder.setUniqueUser(val.users.size());
			builder.setClicks(val.clicks);
			builder.setShowBadgeDetail(val.show_badge_detail);
			builder.setEditContactForm(val.edit_contact_form);
			builder.setSubmitContactForm(val.submit_contact_form);
			
			// set rest part of VinImpressionCount as default values
			builder.setMarketplaceSrps(0L);
			builder.setMarketplaceVdps(0L);
			
			context.write(word, new AvroValue<VinImpressionCounts>(builder.build()));
		}
    	
    	
//    	word.set(vin);
//		
//		VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
//        builder.setUniqueUser(Long.parseLong(session.getUserId().toString()));
//        
//        builder.setShowBadgeDetail(1L);
//        builder.setEditContactForm(1L);
//        builder.setSubmitContactForm(1L);
        
        
        // For each word in the input line, emit a count of 1 for that word.
//        while (tokenizer.hasMoreTokens()) {
//            word.set(tokenizer.nextToken());
//
//            WordCountData.Builder builder = WordCountData.newBuilder();
//            builder.setCount(Utils.ONE);
//            context.write(word, new AvroValue<WordCountData>(builder.build()));
//            context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
//        }
    }
}
