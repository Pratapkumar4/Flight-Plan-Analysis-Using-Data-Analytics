import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlightAnalysis {

    // Mapper Class
    public static class FlightMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row
            if (key.get() == 0) return; // Skip the first row, which is the header

            // Split the CSV data based on commas
            String[] columns = value.toString().split(",");
            if (columns.length < 16) return; // Skip invalid rows (if any)

            String flightDate = columns[2]; // Flight date (for counting flights per day)
            String isRefundable = columns[8]; // Refundable ticket
            String isNonStop = columns[9]; // Non-stop journey
            String isBasicEconomy = columns[7]; // Basic economy
            String startingAirport = columns[3]; // Starting airport

            // Handle the issue where columns might contain multiple values like "1650198000||1650205620"
            int totalFare = parseInteger(columns[12]);
            int seatsRemaining = parseInteger(columns[13]);
            int totalDistance = parseInteger(columns[15]);  // Distance column

            // Debugging the parsed totalDistance value
            System.out.println("Parsed Distance: " + totalDistance);

            // Output key-value pairs based on objectives
            outputKey.set("Total Flights per Day");
            context.write(new Text(flightDate), one);

            // Flights with refundable tickets
            if ("Y".equals(isRefundable)) {
                outputKey.set("Flights with Refundable Tickets");
                context.write(new Text(flightDate), one);
            }

            // Flights with non-stop journeys
            if ("Y".equals(isNonStop)) {
                outputKey.set("Flights with Non-stop Journeys");
                context.write(new Text(flightDate), one);
            }

            // Total distance traveled by flights
            outputKey.set("Total Distance Traveled");
            context.write(new Text("Total Distance"), new IntWritable(totalDistance));

            // Count of flights per starting airport
            outputKey.set("Flights per Starting Airport");
            context.write(new Text(startingAirport), one);

            // Seats remaining per day
            outputKey.set("Seats Remaining per Day");
            context.write(new Text(flightDate), new IntWritable(seatsRemaining));

            // Percentage of flights in Basic Economy
            if ("Y".equals(isBasicEconomy)) {
                outputKey.set("Flights in Basic Economy");
                context.write(new Text("Basic Economy"), one);
            }

            // Average fare per day (add total fare and count later for averaging)
            outputKey.set("Total Fare");
            context.write(new Text(flightDate), new IntWritable(totalFare));
        }

        // Helper method to safely parse integers
        private int parseInteger(String value) {
            try {
                // If the value contains "||", take the first part (before "||")
                String[] parts = value.split("\\|\\|");
                if (parts.length > 0) {
                    return Integer.parseInt(parts[0].trim());  // Take first part before "||"
                }
            } catch (NumberFormatException e) {
                System.out.println("Error parsing value: " + value);
            }
            return 0;  // Return 0 if the value is not a valid integer
        }
    }

    // Reducer Class
    public static class FlightReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        // Job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flight Analysis");
        job.setJarByClass(FlightAnalysis.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);

        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

