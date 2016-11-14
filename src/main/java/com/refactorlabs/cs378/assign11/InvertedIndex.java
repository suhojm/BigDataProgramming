package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.refactorlabs.cs378.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class InvertedIndex {
	final static Map<String, Set<String>> map = new HashMap<String, Set<String>>();
	
	public static class StrComparator implements Comparator<String>{
		@Override
		public int compare(String o1, String o2) {
			
			// TODO Auto-generated method stub
			String[] str1 = o1.split(":");
			String[] str2 = o2.split(":");
			
			int index = 0;
			while(index < str1[0].length() && index < str2[0].length()){
				
				if(str1[0].charAt(index) != str2[0].charAt(index))
					return str1[0].charAt(index) - str2[0].charAt(index);
				
				index++;
			}
			
			int i1 = Integer.parseInt(str1[1]);
			int i2 = Integer.parseInt(str2[1]);
			
			if(i1 != i2)
				return i1 - i2;
			
			i1 = Integer.parseInt(str1[2]);
			i2 = Integer.parseInt(str2[2]);
			
			if(i1 != i2)
				return i1 - i2;
			
			return 0;
		}
	}
	
	public static class ValueComparator implements Comparator<String>, Serializable{

		@Override
		public int compare(String o1, String o2) {
//			System.out.println(o1 + " Size: " + map.get(o1).size() + " " + o2 + " Size: " + map.get(o2).size());
			
			int i = map.get(o2).size() - map.get(o1).size();
			if(i != 0)
				return i;

			int length1 = o1.length();
			int length2 = o2.length();
			int index = 0;
			

			while(index < length1 && index < length2){
				if(o1.charAt(index) != o2.charAt(index))
					return o1.charAt(index) - o2.charAt(index);
				
				index++;
			}

			return length1 - length2;
		}
	}
	
	public static void main(String[] args){
		
		Utils.printClassPath();
		
		String inputFilename = args[0];
		String outputFilename = args[1];
		
		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		FlatMapFunction<String, Tuple2<String,String>> splitFunction =
				new FlatMapFunction<String, Tuple2<String,String>>() {
			@Override
			public Iterable<Tuple2<String, String>> call(String line) throws Exception {
				StringTokenizer tokenizer = new StringTokenizer(line);
				List<Tuple2<String, String>> wordList = Lists.newArrayList();
				Map<String, String> wordMap = Maps.newHashMap();
				
				String theKey = "";
				if(tokenizer.hasMoreTokens()) 
					theKey = tokenizer.nextToken();
				
				while (tokenizer.hasMoreTokens()) {
					String theToken = tokenizer.nextToken();
					//removing the punctuation
					theToken = theToken.replaceAll("--", " ");
					theToken = theToken.replaceAll("(?<=.)(?=\\[.+\\])", " ");
					theToken = theToken.replaceAll("[.,:;?!\"_()]", " ");
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
					wordList.add(new Tuple2(entry.getKey(), entry.getValue()));
				}
				
				return wordList;
			}
		};

		// Transform into word and count
		PairFunction<Tuple2<String,String>, String, Iterable<String>> addCountFunction =
				new PairFunction<Tuple2<String,String>, String, Iterable<String>>() {

			@Override
			public Tuple2<String, Iterable<String>> call(Tuple2<String, String> t) throws Exception {
				// TODO Auto-generated method stub
				String key = t._1;
				String value = t._2;
				List<String> val = Lists.newArrayList();
				val.add(t._2);
				Set<String> s = map.get(key);
				if(s == null)
					s = new HashSet<String>();
				s.add(value);
				map.put(key,s);
				return new Tuple2(key, val);
			}
		};
		
		
		Function2<Iterable<String>, Iterable<String>, Iterable<String>> sumFunction =
				new Function2<Iterable<String>, Iterable<String>, Iterable<String>>() {
		
			@Override
			public Iterable<String> call(Iterable<String> i1, Iterable<String> i2) throws Exception {
				// TODO Auto-generated method stub
				List<String> theList = Lists.newArrayList();
				for(String s : i1){
					theList.add(s);
				}
				for(String s : i2){
					theList.add(s);
				}
				Collections.sort(theList , new StrComparator());
				return theList;
			}
		};

		// All in one line:
//		JavaPairRDD<String, Iterable<String>> inversed =
//                  input.flatMap(splitFunction).mapToPair(addCountFunction).reduceByKey(sumFunction).sortByKey();
		
		
		JavaPairRDD<String, Iterable<String>> inversed =
                input.flatMap(splitFunction).mapToPair(addCountFunction).reduceByKey(sumFunction).sortByKey(new ValueComparator());
		
		
		
		// Save the word count to a text file (initiates evaluation)
		
//		JavaPairRDD<String, Iterable<String>> sortedReversedSummed = reversedSummed.sortByKey(new StrComparator(), true);
		inversed.saveAsTextFile(outputFilename);

		// Shut down the context
		sc.stop();

	}
}
