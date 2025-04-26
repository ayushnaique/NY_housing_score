package profiling;

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
public class HousingProfilerCSV {
    /*
     * This Mapper Reducer pair will be used to output the NULL value counts for each column
     * This data can then be used during cleaning, and the columns with large null values can be dropped
     */
    public static class NullValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Skip the header row
            if (!fields[0].equals("INDEX")) {
                // Emit null counts for each column
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i] == null || fields[i].trim().isEmpty() || fields[i].equals("(null)")) {
                        context.write(new Text("Column" + i), new IntWritable(1));
                    }
                }

                // If the length of the data is not what expected, there are empty cells in the end
                if(fields.length < 42) {
                    for (int j = fields.length; j < 42; j ++) {
                        context.write(new Text("Column" + j), new IntWritable(1));
                    }
                }
            }
        }
    }

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

    /*
     * This Mapper Reducer pair will be used to output the first entry and the last entry date
     * This data can then be used during for querying, where all the data should be in the same date range for better score values
     */
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

    /*
     * This Mapper Reducer pair will be used to output the row number for data with missing coordinates
     */
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

    public static class MissingCoordinatesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key, value); // Output the row number
            }
        }
    }

    /*
     * This Mapper Reducer pair will be used to output the unique possiblities in the column CURRENTSTATUS
     * This data can then be used during cleaning, and the output could be mapped to integers based of severity
     */
    public static class EntryMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Extract the value from column 29 i.e. CURRENTSTATUS
            if(fields.length >= 29) {
                String CurrentStatus = fields[29].toLowerCase();

                context.write(new Text(CurrentStatus), NullWritable.get());
            }
        }
    }

    public static class EntryReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
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
        job3.waitForCompletion(true);

        // Job 4: Unique Entries for CurrentStatus
        Job job4 = Job.getInstance(conf, "Missing Coordinates");
        job4.setJarByClass(DataProfilerCSV.class);
        job4.setMapperClass(EntryMapper.class);
        job4.setReducerClass(EntryReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "_unique_val"));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}