package com.refactorlabs.cs378.assign7;

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// THIS SECOND MAPPER READ THE CSV FILE AS INPUT

// <LongWritable, Text, Text, AvroValue<VinImpressionCounts>


/**
 * Map class for various WordCount examples that use the AVRO generated class WordCountData.
 */
public class CsvMapper extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

        // For each word in the input line, emit a count of 1 for that word.
        while (tokenizer.hasMoreTokens()) {
        	String data = tokenizer.nextToken();
        	String[] datas = data.split(",");

            VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();
            
            if(datas[1].trim().toUpperCase().equals("SRP")){
            	builder.setMarketplaceSrps(Long.parseLong(datas[2].trim()));
            	builder.setMarketplaceVdps(0L);
            }
            else if(datas[1].trim().toUpperCase().equals("VDP")){
            	builder.setMarketplaceSrps(0L);
            	builder.setMarketplaceVdps(Long.parseLong(datas[2].trim()));
            }
            
            // set other values with default values.
            builder.setUniqueUser(0L);
            builder.setClicks(null);
            builder.setShowBadgeDetail(0L);
            builder.setEditContactForm(0L);
            builder.setSubmitContactForm(0L);
            
            // set the String value of VIN as the key 
            word.set(datas[0].trim());
            
            // Write
            context.write(word, new AvroValue<VinImpressionCounts>(builder.build()));
            context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
        }
    }
}
