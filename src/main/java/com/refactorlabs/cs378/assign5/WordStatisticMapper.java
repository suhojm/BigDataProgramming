package com.refactorlabs.cs378.assign5;

import com.google.common.collect.Maps;
import com.refactorlabs.cs378.assign4.WordStatisticWritable;
import com.refactorlabs.cs378.assign5.Utils;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Map class for various WordCount examples that use the AVRO generated class WordStatisticData.
 */
public class WordStatisticMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        Map<String, Integer> wordCountMap = Maps.newHashMap();
        
        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        // For each word in the input line, emit a count of 1 for that word.
        while (tokenizer.hasMoreTokens()) {
            String theToken = tokenizer.nextToken();
            
            //removing the punctuations
			theToken = theToken.replaceAll("--", " ");
			theToken = theToken.replaceAll("(?<=.)(?=\\[.+\\])", " ");
			theToken = theToken.replaceAll("[.,:;?!\"_]", " ");
			theToken = theToken.trim();

			String[] strs = theToken.split("\\s+");
			
			// count the number word appears for a paragraph
			for(String str : strs){
				if(str != "" && str != " "){
					if(wordCountMap.containsKey(str)){
						int cnt = wordCountMap.get(str);
						wordCountMap.put(str, ++cnt);
					}
					else
						wordCountMap.put(str, Utils.initialCount);
				}
			}
		}
		for(Map.Entry<String, Integer> entry : wordCountMap.entrySet()){
			long cnt = entry.getValue().intValue();
			word.set(entry.getKey());
			
			WordStatisticData.Builder builder = WordStatisticData.newBuilder();
            builder.setDocumentCount(Utils.ONE);
			builder.setTotalCount(cnt);
			builder.setSumOfSquares(cnt*cnt);
			builder.setMean(0.0);
			builder.setVariance(0.0);
			context.write(word, new AvroValue<WordStatisticData>(builder.build()));
            context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
		}
    }
}
