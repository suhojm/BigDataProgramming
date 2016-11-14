package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.assign4.WordStatisticWritable;
import com.refactorlabs.cs378.assign5.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * WordCount example using AVRO defined class for the word count data,
 * to demonstrate how to use AVRO defined objects.
 */
public class WordStatistic extends Configured implements Tool {

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticData>,
            Text, AvroValue<WordStatisticData>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticData>> values, Context context)
                throws IOException, InterruptedException {

            context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

            long docCnt = 0L;
            long totCnt = 0L;
            long sumSqr = 0L;
            
            
            // Sum up the counts for the current word, specified in object "key".
            for (AvroValue<WordStatisticData> value : values) {
            	
                docCnt += value.datum().getDocumentCount();
                totCnt += value.datum().getTotalCount();
                sumSqr += value.datum().getSumOfSquares();
            }
            
            double mean = (double)totCnt / (double)docCnt; // mean
            double variance = ((double)mean*mean) * docCnt + (-2 * (double)mean * (double)totCnt) + (double)sumSqr;
            variance = variance / docCnt; // variance
            
            // Emit the total count for the word.
            WordStatisticData.Builder builder = WordStatisticData.newBuilder();
            builder.setDocumentCount(docCnt);
            builder.setTotalCount(totCnt);
            builder.setSumOfSquares(sumSqr);
            builder.setMean(mean);
            builder.setVariance(variance);
            
            context.write(key, new AvroValue<WordStatisticData>(builder.build()));            
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatistic <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "WordStatistic");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistic.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordStatisticMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticData.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
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
        int res = ToolRunner.run(new Configuration(), new WordStatistic(), args);
        System.exit(res);
    }

}