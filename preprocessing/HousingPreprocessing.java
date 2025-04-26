package preprocessing;

// Import libraries for Driver
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Import libraries for Mapper and Reducer
import java.util.Date;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class HousingPreprocessing {
    /*
     * This Mapper and Reducer pair is used to output the frequency of the occurrence of the number of parts
     * Due to the data being dirty, a column contains quotes in which there are commas
     * The number of quoted lines is unknown
     * This pair outputs the number of parts the line gets split into using the ' " ' delimiter
     * Using this code, we found out the number of parts.
     */
    public static class SplitMapperCount extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("ViolationID")) { // Skip header
                // Emit the first part of the split
                return;
            }
            
            context.write(new IntWritable(fields.length), new IntWritable(1)); // Emit IntWritable
        }
    }

    public static class SplitReducerCount extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    /*
     * This Mapper and Reducer pair is used to output the line along with the number of parts it gets split into
     * The Mapper will emit the total number of parts in the line that is split by the ' " ' delimiter
     * The reducer will emit the lines that have 3 parts
     */
    public static class SplitMapperCounter extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("ViolationID")) { // Skip header
                return;
            }
            
            context.write(new IntWritable(fields.length), value); // Emit IntWritable
        }
    }

    public static class SplitReducerCounter extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key.get() == 3) {
                for (Text value : values) {
                    context.write(NullWritable.get(), value); // Emit TextWritable
                }
            }
        }
    }

    /*
     * This Mapper and Reducer pair is used to clean the Housing Maintenance Violations data
     * The Mapper will emit the total number of parts in the line that is split by the ' " ' delimiter
     * Using this data the Reducer will operate on the lines that have 3 or 1 parts
     * if there are 3 parts, it will remove the commas from the quoted part
     * and make the string lower case
     * if there is only 1 part, it will make the string lower case
     * The output will be the cleaned data in the form of TextWritable
    */
    public static class CleanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("ViolationID")) { // Skip header
                return;
            }

            context.write(new IntWritable(fields.length), value); // Emit Text along with the total parts, ideally should be 3 or 1
        }
    }

    public static class CleanReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // If there are no quotes in the line, emit the line
            if(key.get() == 1) {
                for (Text value : values) {
                    String cleanedString = value.toString().toLowerCase(); // make lower case
                    context.write(new Text(cleanedString), NullWritable.get()); // Emit Text as is
                }
            } else if(key.get() == 3) {
                for (Text value : values) {
                    // Split the text by the "\"" delimiter
                    String[] fields = value.toString().split("\"");
                    String cleanedString = fields[0] + fields[1].replace(",", "") + fields[2]; // Remove commas from the quoted part
                    // make lower case
                    cleanedString = cleanedString.toLowerCase();
                    context.write(new Text(cleanedString), NullWritable.get()); // Emit cleaned Text
                }
            }
        }
    }

    // Driver to run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Housing Cleaning");
        job.setJarByClass(HousingPreprocessing.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_preprocessed"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}