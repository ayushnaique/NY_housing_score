package cleaning;

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

public class CleanerShootingData {
    /*
     * This Mapper and Reducer will
     * 1) Remove columns that have plenty of null values
     * 2) Remove any other unnecessary columns
     * 3) Convert Column10: STATISTICAL_MURDER_FLAG to 0(No), 1(Yes), 2(unknown)
     * 4) Convert the String to lowercase
     * 
     * 
     * According to the data profiling output
     * Columns 12 and 13 have 10938 null values
     * Column 5 has 25596 null values
     * Column 11 has 10972 null values
     * Column 9 has 17503 null values
     * Column 8 has 25603 null values
     * 
     * 
     * Unnecessary columns:
     * Column0: INDEX
     * Column1: INCIDENT_KEY
     * Column3: OCCUR_TIME
     * Column6: PRECINCT
     * Column7: JURISDICTION_CODE
     * Column14: VIC_AGE_GROUP
     * Column15: VIC_SEX
     * Column16: VIC_RACE
     * 
     * 
     * The MapReduce code can just split the work among the mapper and the reducer
     * The Mapper can Drop the unnecessary columns can OneHotEncode STATISTICAL_MURDER_FLAG
     * The Reducer can convert the string to lowercase
     * 
     */
    public static class CleanerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1); // Split CSV while preserving quoted values

            // Skip the header
            if (fields[0].equals("INDEX") || fields.length < 19) { // Skip header or if coords are missing
                return;
            }

            String OutputText = "";
            for (int i = 0; i < fields.length; i++) {
                if (i == 0 || i == 1 || i == 3 || i == 5 || i == 6 || i == 7 || i == 8 || 
                    i == 9 || i == 11 || i == 12 || i == 13 || i == 14 || i == 15 || i == 16) {
                    continue; // Skip the unnecessary columns
                }
                if (i == 10) { // Encode the Murder flag
                    if (fields[i].toLowerCase().equals("false"))
                        OutputText += "0";
                    else if (fields[i].toLowerCase().equals("true"))
                        OutputText += "1";
                    else
                        OutputText += "2";
                } else if (i < fields.length) { // Make sure that the field exists
                    if (fields[i] == null || fields[i].trim().isEmpty() || fields[i].equals("(null)")) {
                        OutputText += "(null)"; // Add null to the output
                    } else {
                        // Remove commas from quoted values
                        String cleanedField = fields[i].replaceAll("^\"|\"$", "") // Remove surrounding quotes
                                                       .replace(",", "");      // Remove commas inside the quoted value
                        OutputText += cleanedField;
                    }
                }
                if (i != fields.length - 1) {
                    OutputText += ","; // Add a comma to separate the columns
                }
            }

            // Emit the semi-cleaned data
            context.write(new Text(OutputText), NullWritable.get());
        }
    }

    public static class CleanerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString().toLowerCase(); // Convert the string to lowercase
            String[] fields = line.split(",", -1); // Splits the CSV file into its constituent cells

            // Ensure the fields array has enough elements
            if (fields.length <= 5) {
                return; // Skip lines with insufficient fields
            }

            // Dont emit the lines where the longitude and latitude are missing
            if(fields[6] == null || fields[6].equals("(null)") || fields[6].trim().isEmpty() ||
               fields[7] == null || fields[7].equals("(null)") || fields[7].trim().isEmpty()) {
                return;
            }

            context.write(new Text(line), NullWritable.get()); // Emit the cleaned data
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Emit the header
            String Header = "OCCUR_DATE,BORO,STATISTICAL_MURDER_FLAG,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lon_Lat";
            context.write(new Text(Header), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaner Shooting Data");
        job.setJarByClass(CleanerShootingData.class);
        job.setMapperClass(CleanerMapper.class);
        job.setReducerClass(CleanerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1); // Ensure only one reducer task to avoid duplicate headers
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_cleaned"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
