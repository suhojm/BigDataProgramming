package com.refactorlabs.cs378.assign12;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.refactorlabs.cs378.assign11.InvertedIndex;
import com.refactorlabs.cs378.assign11.InvertedIndex.StrComparator;
import com.refactorlabs.cs378.assign11.InvertedIndex.ValueComparator;
import com.refactorlabs.cs378.assign6.Event;
import com.refactorlabs.cs378.assign6.EventSubtype;
import com.refactorlabs.cs378.assign6.EventType;
import com.refactorlabs.cs378.assign8.SessionType;
import com.refactorlabs.cs378.utils.Utils;

import scala.Serializable;
import scala.Tuple2;

public class UserSession {
	
	public static class CustomPartitioner extends Partitioner {
		private int numPartition;

		public CustomPartitioner(int numParts) {
			numPartition = numParts;
		}

		@Override
		public int getPartition(Object key) {
			String domain = (((Tuple2)key)._2).toString();
			int n = domain.hashCode() % numPartitions();
			if (n < 0) {
				return n + numPartitions();
			} else {
				return n;
			}
		}

		@Override
		public int numPartitions() {
			return numPartition;
		}

	}
	
	public enum EventType {
		CHANGE, CLICK, CONTACT_FORM_STATUS, DISPLAY,
        EDIT, ILMR_STATUS, ILMR_CPO, PLAY, PRINT,
    	SHOW, SUBMIT, VISIT
	}

	public enum SessionType {
		SUBMITTER, CPO, CLICKER, SHOWER, VISITOR, OTHER
	}
	
	private static class Event implements Serializable { 
		String eventType;
		String eventTimestamp; 
		EventType et;
		public String toString() { return "<" + eventType + "," + eventTimestamp + ">";} 
		}
	
	public static class KeyComparator implements Comparator<Tuple2<String, String>>, Serializable{

		@Override
		public int compare(Tuple2<String, String> t1, Tuple2<String, String> t2) {
			// TODO Auto-generated method stub
			
			String userid1 = t1._1;
			String userid2 = t2._1;
			
			int index = 0;
			while(index < userid1.length() && index < userid2.length()){
				
				if(userid1.charAt(index) != userid2.charAt(index))
					return userid1.charAt(index) - userid2.charAt(index);
				
				index++;
			}
			
			if(userid1.length() != userid2.length())
				return userid1.length() - userid2.length();
			
			String rd1 = t1._2;
			String rd2 = t2._2;
			
			index = 0;
			while(index < rd1.length() && index < rd2.length()){
				if(rd1.charAt(index) != rd2.charAt(index))
					return rd1.charAt(index) - rd2.charAt(index);
				
				index++;
			}
			
			if(rd1.length() != rd2.length())
				return rd1.length() - rd2.length();
			
			return 0;
		}
		
	}
	
	public static class EventTSComparator implements Comparator<Event>{

		@Override
		public int compare(Event e1, Event e2) {
			
			String ts1 = e1.eventTimestamp;
			String ts2 = e2.eventTimestamp;
			
			//2015-09-30 14:02:57.000000
			
			String[] date_time1 = ts1.split(" ");
			String[] date_time2 = ts2.split(" ");
			
			String[] date1 = date_time1[0].split("-");
			String[] date2 = date_time2[0].split("-");
			
			int index = 0;
			while(index < date1.length && index < date2.length){
				int i1 = Integer.parseInt(date1[index]);
				int i2 = Integer.parseInt(date2[index]);
				
				if(i1!=i2){
					return i1 - i2; 
				}
				index++;
			}
			
			String[] time1 = date_time1[1].split(":");
			String[] time2 = date_time2[1].split(":");
			
			if(Integer.parseInt(time1[0]) != Integer.parseInt(time2[0]))
				return Integer.parseInt(time1[0]) - Integer.parseInt(time2[0]);
				
			if(Integer.parseInt(time1[1]) != Integer.parseInt(time2[1]))
				return Integer.parseInt(time1[1]) - Integer.parseInt(time2[1]);
			
			if(Double.parseDouble(time1[2]) != Double.parseDouble(time2[2])){
				double d = Double.parseDouble(time1[2]) - Double.parseDouble(time2[2]);
				if(d > 0)
					return 1;
				if(d < 0)
					return -1;
			}
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
	
	final static Map<String, Set<String>> map = new HashMap<String, Set<String>>();
	public static void main(String[] args){
		
		Utils.printClassPath();
		
		String inputFilename = args[0];
		String outputFilename = args[1];
		
		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);
		final Accumulator<Integer> eventCount = sc.accumulator(0);
		final Accumulator<Integer> sessionCount = sc.accumulator(0);
		final Accumulator<Integer> otherCount = sc.accumulator(0);
		final Accumulator<Integer> deletedOtherCount = sc.accumulator(0);
		
		PairFunction<String, Tuple2<String,String>, Iterable<Event>> mapToPairFunction =
				new PairFunction<String, Tuple2<String, String>, Iterable<Event>>() {

			@Override
			public Tuple2<Tuple2<String,String>, Iterable<Event>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				
				String inputLine = line;
				String[] tokens = inputLine.split("\t");
				
				// Userid: tokens[0]
				// Referring Domain: tokens[4]
				// Event Type: tokens[1]
				// Event TimeStamp: tokens[5]
				// <<UserId, Referring Domain> , Event<Type,TimeStamp>>
				
				List<Event> theList = Lists.newArrayList();
				Event e = new Event();
				e.eventType = tokens[1];
				e.eventTimestamp = tokens[5];
				theList.add(e);
				
				if(e.eventType.equals("change contact form type")){
					e.et = (EventType.CHANGE);
				} else if(e.eventType.equals("click alternative")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click contact banner")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click contact button")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click dealer phone")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click features section")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click get directions")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click vehicle history report link")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("click_on_alternative")){
					e.et = (EventType.CLICK);
				} else if(e.eventType.equals("contact form error")){
					e.et = (EventType.CONTACT_FORM_STATUS);
				} else if(e.eventType.equals("contact form success")){
					e.et = (EventType.CONTACT_FORM_STATUS);
				} else if(e.eventType.equals("alternatives_displayed")){
					e.et = (EventType.DISPLAY);
				} else if(e.eventType.equals("display_ilmr_report_listing")){
					e.et = (EventType.DISPLAY);
				} else if(e.eventType.equals("edit contact form")){
					e.et = (EventType.EDIT);
				} else if(e.eventType.equals("error_loading_ilmr")){
					e.et = (EventType.ILMR_STATUS);
				} else if(e.eventType.equals("ilmr_cpo_financing")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("ilmr_cpo_inspection")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("ilmr_cpo_roadside")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("ilmr_cpo_sirius")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("ilmr_cpo_warranty")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("ilmr_see_more_cpo")){
					e.et = (EventType.ILMR_CPO);
				} else if(e.eventType.equals("play_video_ilmr")){
					e.et = (EventType.PLAY);
				} else if(e.eventType.equals("print_ilmr")){
					e.et = (EventType.PRINT);
				} else if(e.eventType.equals("show badge detail")){
					e.et = (EventType.SHOW);
				} else if(e.eventType.equals("show photo modal")){
					e.et = (EventType.SHOW);
				} else if(e.eventType.equals("submit contact form")){
					e.et = (EventType.SUBMIT);
				} else if(e.eventType.equals("visit alternatives")){
					e.et = (EventType.VISIT);
				} else if(e.eventType.equals("visit badges")){
					e.et = (EventType.VISIT);
				} else if(e.eventType.equals("visit contact form")){
					e.et = (EventType.VISIT);
				} else if(e.eventType.equals("visit features")){
					e.et = (EventType.VISIT);
				} else if(e.eventType.equals("visit vehicle history")){
					e.et = (EventType.VISIT);
				} else if(e.eventType.equals("visit_market_report_listing")){
					e.et = (EventType.VISIT);
				} else{
					e.et = (null);
				}

				
				return new Tuple2(new Tuple2(tokens[0], tokens[4]), theList);
			}
		};
		
		
		
		Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>> reduceFunction =
				new Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>>() {
		
			@Override
			public Iterable<Event> call(Iterable<Event> t0, Iterable<Event> t1) throws Exception {
				// TODO Auto-generated method stub
				List<Event> theList = Lists.newArrayList();
				for(Event e : t0){
					theList.add(e);
				}
				for(Event e : t1){
					theList.add(e);
				}
				Collections.sort(theList , new EventTSComparator());
				
				return theList;
			}
		};
		
		Function<Tuple2<Tuple2<String,String>, Iterable<Event>>, Boolean> filterFunction = 
			new Function<Tuple2<Tuple2<String,String>, Iterable<Event>>, Boolean>(){

				@Override
				public Boolean call(Tuple2<Tuple2<String, String>, Iterable<Event>> t) throws Exception {
					// TODO Auto-generated method stub
					
					SessionType stype = null;
					
					for(Event e : t._2){
						if(e.et == EventType.CHANGE || e.et == EventType.CONTACT_FORM_STATUS || e.et == EventType.EDIT || e.et == EventType.SUBMIT){
			    			stype = SessionType.SUBMITTER;
			    		}
			    		else if(e.et == EventType.ILMR_CPO){
			    			if(stype != SessionType.SUBMITTER){
			    				stype = SessionType.CPO;
			    			}
			    		}
			    		else if(e.et == EventType.CLICK || e.et == EventType.PLAY || e.et == EventType.PRINT){
			    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO){
				    			stype = SessionType.CLICKER;
			    			}
			    		}
			    		else if(e.et == EventType.SHOW){
			    			if(stype != SessionType.SUBMITTER && stype != SessionType.CPO && stype != SessionType.CLICKER){
			    				stype = SessionType.SHOWER;
			    			}
			    		}
			    		else if(e.et == EventType.VISIT){
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
					Random rands = new Random();
					if(t._1._1.toString().equals("user_id")){
						return false;
					}
					
					if(stype != SessionType.OTHER) {
						for(Event e : t._2)
							eventCount.add(1);
						sessionCount.add(1);
						return true;
			    	}
					else{
						for(Event e : t._2)
							eventCount.add(1);
						sessionCount.add(1);
						otherCount.add(1);
						if(rands.nextDouble() < 0.001)
							return true;
						else{
							deletedOtherCount.add(1);
							return false;
						}
					}
				}
		};
		
		
	
		JavaPairRDD<Tuple2<String,String>, Iterable<Event>> result = 
				input.mapToPair(mapToPairFunction).reduceByKey(reduceFunction).sortByKey(new KeyComparator()).filter(filterFunction).partitionBy(new CustomPartitioner(8));
		
		// Save the word count to a text file (initiates evaluation)
		

		result.saveAsTextFile(outputFilename);
		
		System.out.println("Total number of events: " + eventCount.value());
		System.out.println("Total number of sessions: " + sessionCount.value());
		System.out.println("Total number of sessions of type OTHER: " + otherCount.value());
		System.out.println("Total number of sessions of type OTHER that were filtered out: " + deletedOtherCount.value());
		
		
		
		// Shut down the context
		sc.stop();

	}
}
