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
    // Mapper to split the data using the ' " ' delimiter
    public static class SplitMapperCount extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("INDEX")) { // Skip header
                // Emit the first part of the split
                return;
            }

            int index = Integer.parseInt(parts[0]);
            
            context.write(new IntWritable(fields.length), new IntWritable(1)); // Emit IntWritable
        }
    }

    // Reducer to output the frequency of number of parts
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

    // Mapper to split the data using the ' " ' delimiter and emit the parts
    public static class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("INDEX")) { // Skip header
                // Emit the first part of the split
                return;
            }

            for(int i=0; i<fields.length; i++) {
                context.write(new Text("Part" + i + ":"), new IntWritable(1)); // Emit TextWritable
            }
        }
    }

    // Reducer to output the count of each part
    public static class SplitReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    // Mapper to output (total parts, line)
    public static class SplitMapperCounter extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("INDEX")) { // Skip header
                return;
            }
            
            context.write(new IntWritable(fields.length), value); // Emit IntWritable
        }
    }

    // Reducer to output the line only when the total parts are equal to 3
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

    // Mapper for cleaning the Housing data
    public static class CleanMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\"");
            String[] parts = fields[0].split(","); // Split by comma

            if(parts[0].equals("INDEX")) { // Skip header
                return;
            }

            for(int i=0; i<fields.length; i++) {
                context.write(new IntWritable(1), value); // Emit Text along with the total parts, ideally should be 3 or 1
            }
        }
    }

    // Reducer to output the cleaned data
    public static class CleanReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // If there are no quotes in the line, emit the line
            if(key.get() == 1) {
                for (Text value : values) {
                    context.write(value, NullWritable.get()); // Emit Text as is
                }
            } else if(key.get() == 3) {
                for (Text value : values) {
                    // Clean the quoted data, remove all commas
                    String cleanedString = value.split("\"")[1].replaceAll(",", "");
                    // make lower case
                    cleanedString = cleanedString.toLowerCase();
                    context.write(new Text(cleanedString), NullWritable.get()); // Emit cleaned Text
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Append the Header to the output file
            String header = "INDEX,ViolationID,BuildingID,RegistrationID,BoroID,Borough,HouseNumber,LowHouseNumber,HighHouseNumber,StreetName,StreetCode,Postcode,Apartment,Story,Block,Lot,Class,InspectionDate,ApprovedDate,OriginalCertifyByDate,OriginalCorrectByDate,NewCertifyByDate,NewCorrectByDate,CertifiedDate,OrderNumber,NOVID,NOVDescription,NOVIssuedDate,CurrentStatusID,CurrentStatus,CurrentStatusDate,NovType,ViolationStatus,RentImpairing,Latitude,Longitude,CommunityBoard,CouncilDistrict,CensusTract,BIN,BBL,NTA";
            context.write(new Text(header), NullWritable.get()); // Emit header
        }
    }

    // Driver to run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Housing Cleaning");
        job.setJarByClass(HousingPreprocessing.class);
        job.setMapperClass(CleanMapper.class);
        job.setCombinerClass(CleanReducer.class);
        job.setReducerClass(CleanReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_cleaned"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}