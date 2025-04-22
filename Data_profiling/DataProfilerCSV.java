package Data_profiling;

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

public class DataProfilerCSV {
    // Mapper for First Date and Last DateD
    public static class DateMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (!fields[2].equals("OCCUR_DATE")) { // Skip header
                context.write(new Text("FirstDate"), new Text(fields[2]));
                context.write(new Text("LastDate"), new Text(fields[2]));
            }
        }
    }

    // Reducer for First Date and Last Date
    public static class DateReducer extends Reducer<Text, Text, Text, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Date resultDate = null;

            for (Text value : values) {
                try {
                    Date currentDate = dateFormat.parse(value.toString());
                    if (resultDate == null || (key.toString().equals("FirstDate") && currentDate.before(resultDate)) ||
                            (key.toString().equals("LastDate") && currentDate.after(resultDate))) {
                        resultDate = currentDate;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            context.write(key, new Text(dateFormat.format(resultDate)));
        }
    }

    // Mapper for Null Value Count
    public static class NullValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (!fields[0].equals("INDEX")) { // Skip header
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i] == null || fields[i].trim().isEmpty() || fields[i].equals("(null)")) {
                        context.write(new Text("Column" + i), new IntWritable(1));
                        // The mapper misses the data where the last 3 columns are missing as it is a csv file
                    }
                }
                // If the row has less than 22 columns, count the missing columns
                if (fields.length < 22) {
                    for (int j = fields.length; j < 22; j++) {
                        context.write(new Text("Column" + j), new IntWritable(1));
                    }
                }
            }
        }
    }

    // Reducer for Null Value Count
    public static class NullValueReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Mapper for Missing Coordinates
    public static class MissingCoordinatesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
    
            if(!fields[0].equals("INDEX")) { // Skip header
                // Get the index value of the row
                int index = Integer.parseInt(fields[0]);
                // Check if the row has at least 21 columns (0-based index for Latitude and Longitude is 19 and 20)
                if (fields.length > 21) { // Ensure column 19 (Latitude) and 20 (Longitude) exists
                    String latitude = fields[19];
                    String longitude = fields[20]; 
        
                    // If either Latitude or Longitude is missing or empty, classify as MissingCoordinatesRow
                    if (latitude == null || latitude.trim().isEmpty() || longitude == null || longitude.trim().isEmpty()) {
                        context.write(new Text("MissingCoordinatesRow"), new IntWritable(index));
                    }
                } else {
                    // If columns 19 and 20 are missing, classify as MissingCoordinatesRow
                    context.write(new Text("MissingCoordinatesRow"), new IntWritable(index));
                }
            }
        }
    }

    // Reducer for Missing Coordinates
    public static class MissingCoordinatesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: First Date and Last Date
        Job job1 = Job.getInstance(conf, "First and Last Date");
        job1.setJarByClass(DataProfilerCSV.class);
        job1.setMapperClass(DateMapper.class);
        job1.setCombinerClass(DateReducer.class); // Combiner will be the same as Reducer
        job1.setReducerClass(DateReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_dates"));
        job1.waitForCompletion(true);

        // Job 2: Null Value Count
        Job job2 = Job.getInstance(conf, "Null Value Count");
        job2.setJarByClass(DataProfilerCSV.class);
        job2.setMapperClass(NullValueMapper.class);
        job2.setCombinerClass(NullValueReducer.class); // Combiner will be the same as Reducer
        job2.setReducerClass(NullValueReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_nulls"));
        job2.waitForCompletion(true);

        // Job 3: Missing Coordinates
        Job job3 = Job.getInstance(conf, "Missing Coordinates");
        job3.setJarByClass(DataProfilerCSV.class);
        job3.setMapperClass(MissingCoordinatesMapper.class);
        job3.setReducerClass(MissingCoordinatesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_missing_coords"));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
