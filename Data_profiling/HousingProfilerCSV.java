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
public class HousingProfilerCSV {

    // Mapper for Null Value Count
    public static class NullValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip the header row
            if (!fields[0].equals("INDEX")) {
                // Emit null counts for each column
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i] == null || fields[i].trim().isEmpty() || fields[i].equals("(null)")) {
                        context.write(new Text("Column" + i), new IntWritable(1));
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

    // Mapper for First and Last Date
    public static class DateMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip the header row
            if (!fields[0].equals("INDEX")) {
                // Emit inspectiondate for first and last date calculation
                if (fields.length > 16) { // Ensure inspectiondate column exists
                    String inspectionDate = fields[17]; // Remove quotes
                    context.write(new Text("FirstDate"), new Text(inspectionDate));
                    context.write(new Text("LastDate"), new Text(inspectionDate));
                }
            }

            
        }
    }

    // Reducer for First and Last Date
    public static class DateReducer extends Reducer<Text, Text, Text, Text> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Date resultDate = null;

            for (Text value : values) {
                try {
                    String dateString = value.toString();
                    if (dateString.isEmpty()) {
                        continue; // Skip empty or invalid dates
                    }
                    // Parse the date string
                    Date currentDate = dateFormat.parse(dateString);
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

    // Mapper for Missing Coordinates
    public static class MissingCoordinatesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Skip the header row
            if (!fields[0].equals("INDEX")) {
                int index = Integer.parseInt(fields[0]); // Get the row number

                // Check if latitude and longitude are null or empty
                if (fields.length > 36) { // Ensure latitude and longitude columns exist
                    String latitude = fields[34];
                    String longitude = fields[35];

                    if (latitude == null || latitude.trim().isEmpty() || latitude.equals("(null)") ||
                            longitude == null || longitude.trim().isEmpty() || longitude.equals("(null)")) {
                                context.write(new Text("MissingCoordinatesRow"), new IntWritable(index)); // Emit row number
                    }
                } else {
                    // If columns 34 and 35 are missing, classify as MissingCoordinatesRow
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
                context.write(key, value); // Output the row number
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: Null Value Count
        Job job1 = Job.getInstance(conf, "Null Value Count");
        job1.setJarByClass(HousingProfilerCSV.class);
        job1.setMapperClass(NullValueMapper.class);
        job1.setCombinerClass(NullValueReducer.class); // Combiner will be the same as Reducer
        job1.setReducerClass(NullValueReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_nulls"));
        job1.waitForCompletion(true);

        // Job 2: First and Last Date
        Job job2 = Job.getInstance(conf, "First and Last Date");
        job2.setJarByClass(HousingProfilerCSV.class);
        job2.setMapperClass(DateMapper.class);
        job2.setCombinerClass(DateReducer.class); // Combiner will be the same as Reducer
        job2.setReducerClass(DateReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_dates"));
        job2.waitForCompletion(true);

        // Job 3: Missing Coordinates
        Job job3 = Job.getInstance(conf, "Missing Coordinates");
        job3.setJarByClass(HousingProfilerCSV.class);
        job3.setMapperClass(MissingCoordinatesMapper.class);
        job3.setReducerClass(MissingCoordinatesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "_missing_coords"));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}