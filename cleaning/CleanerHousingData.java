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

public class CleanerHousingData {
    /*
     * This Mapper and Reducer will
     * 1) Remove columns that have plenty of null values
     * 2) Remove any other unnecessary columns
     * 3) Convert Column29: CurrentStatus to an integer value in the range 0-10 based on severity
     * 4) Convert Column16:  Class to an integer value (1, 2 or 3)
     * 5) Convert the String to lowercase
     * 
     * 
     * According to data profiling, the columns with high null values are:
     * Column12, Column13, Column19, Column20, Column21, Column22, Column23
     * Column25, Column27 and Column31 which will be dropped as they do not provide 
     * insights for our use case
     */
    public static class CleanerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",", -1);

            // Skip the header or if data is missing
            if(fields[0].equals("INDEX") || fields.length < 42) {
                return;
            }

            String OutputText = "";
            for(int i = 0; i < fields.length; i++) {
                if((i >= 0 && i <= 4) || (i >= 6 && i <= 8) || i == 10 || (i >= 12 && i <= 15) ||
                   (i >= 18 && i <= 28) || i == 30 || i == 31 || (i >= 36)) {
                    continue; // Skip the unnecessary fields
                }
                if(i == 16) {
                    String cls = fields[16].trim().toLowerCase();
                    if(cls.equals("a"))       // Lowest
                        OutputText += "1";
                    else if(cls.equals("b"))  // Medium
                        OutputText += "2";
                    else if(cls.equals("c"))  // Highest
                        OutputText += "3";
                    else                               // Unknown
                        OutputText += "0";
                }
                // Convert Column29 into integer values
                else if(i == 29) {
                    String status = fields[29].toLowerCase().trim();  // Normalize input
                    int severity;

                    switch (status) {
                        case "violation closed":
                            severity = 0;
                            break;
                        case "violation dismissed":
                            severity = 1;
                            break;
                        case "certification postponment granted":
                        case "nov certified on time":
                            severity = 2;
                            break;
                        case "nov certified late":
                            severity = 3;
                            break;
                        case "certification postponment denied":
                        case "complied in access area":
                            severity = 4;
                            break;
                        case "currentstatus":
                        case "notice of issuance sent to tenant":
                            severity = 5;
                            break;
                        case "defect letter issued":
                        case "lead docs submitted not acceptable":
                        case "info nov sent out":
                        case "nov sent out":
                            severity = 6;
                            break;
                        case "first no access to re- inspect violation":
                            severity = 7;
                            break;
                        case "second no access to re-inspect violation":
                        case "false certification":
                        case "invalid certification":
                            severity = 8;
                            break;
                        case "not complied with":
                        case "downgrade pending inspection":
                        case "violation open":
                        case "violation will be reinspected":
                            severity = 9;
                            break;
                        case "civ14 mailed":
                        case "violation reopen":
                            severity = 10;
                            break;
                        default:
                            severity = -1; // unknown or missing status
                    }

                    // Replace the original text value with the integer severity
                    OutputText += String.valueOf(severity);
                }
                else if(i == 32 || i == 33) {
                    String val = fields[i].toLowerCase().trim();
                    if(val.equals("open") || val.equals("y"))
                        OutputText += "1";
                    else if(val.equals("close") || val.equals("n"))
                        OutputText += "0";
                    else
                        OutputText += "-1";     // The data is unknown
                }
                else {
                    if (fields[i] == null || fields[i].trim().isEmpty() || fields[i].equals("(null)")) {
                        OutputText += "(null)"; // Add null to the output
                    } else {
                        // Remove commas from quoted values
                        String cleanedField = fields[i].replaceAll("^\"|\"$", "") // Remove surrounding quotes
                                                       .replace(",", "");        // Remove commas inside the quoted value
                        OutputText += cleanedField;
                    }
                }
                
                // No check required as another cell is added in Reducer
                OutputText += ","; // Add a comma to separate the columns
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
            if (fields.length < 10) {
                return; // Skip lines with insufficient fields
            }

            // Dont emit the lines where the longitude and latitude are missing
            if(fields[9] == null || fields[9].equals("(null)") || fields[9].trim().isEmpty() ||
               fields[8] == null || fields[8].equals("(null)") || fields[8].trim().isEmpty()) {
                return;
            }

            String POINT = "POINT (" + fields[8] + " " + fields[9] + ")"; // Create the Point coordinate as the last col

            context.write(new Text(line + POINT), NullWritable.get()); // Emit the cleaned data
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Emit the header
            String Header = "Borough,StreetName,Postcode,Class,InspectionDate,CurrentStatus,ViolationStatus,RentImpairing,Latitude,Longitude,POINT";
            context.write(new Text(Header), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Cleaner Housing Violations Data");
        job.setJarByClass(CleanerHousingData.class);
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


