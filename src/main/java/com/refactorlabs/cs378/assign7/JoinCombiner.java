package com.refactorlabs.cs378.assign7;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Maps;

public class JoinCombiner extends Reducer<Text, AvroValue<VinImpressionCounts>,
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