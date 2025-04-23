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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class HousingPreprocessing {
    // Mapper to split the data using the ' " ' delimiter
    public static class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            for (int i=0; i < fields.length; i++) {
                String finalField = "Part" + String.valueOf(i);
                context.write(new Text(finalField), new IntWritable(1));
            }
        }
    }

    // Reducer to output the quoted data
    public static class SplitReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            context.write(key, new Text("Total: " + total));
        }
    }

    // Driver to run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Housing Preprocessing");
        job.setJarByClass(HousingPreprocessing.class);
        job.setMapperClass(SplitMapper.class);
        job.setCombinerClass(SplitReducer.class);
        job.setReducerClass(SplitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]) + "_parts");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}