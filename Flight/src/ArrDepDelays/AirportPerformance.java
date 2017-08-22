package ArrDepDelays;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;



public class AirportPerformance {

	public static class AirportPerformanceMapper extends Mapper<Object, Text, AirlineOrgTextPair, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		
		/**
		 * Key : Byte offset from where the line is being read value : string
		 * representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			AirlineOrgTextPair key = new AirlineOrgTextPair();
			Text delayMinutes = new Text();

			if (line.length > 0 && isValidEntry(line)) {

				// Set (FlightDate,IntermediateAirPort) as key
				key.setAirLineName(line[8]);
				key.setOrigin(line[16].trim());
				
				

				int sum = (line[15].trim().equals("NA") ? 0 : Integer.parseInt(line[15]));
				
				delayMinutes.set(String.valueOf(sum));

				context.write(key, delayMinutes);
			}
		}

		/**
		 * Function determines the validity of the input record
		 * 
		 * @param data
		 * @return
		 */
		private boolean isValidEntry(String[] record) {

			if (record == null || record.length == 0) {
				return false;
			}
			if (record[0].equals("Year"))
				return false;
			// If any of required field is missing, we'll ignore the record
			if (record[0].isEmpty() || record[2].isEmpty() || record[17].isEmpty()) {
				return false;
			}

			// If flight was cancelled
			if (record[22].equals("1")) {
				return false;
			}

			// Whether flight was cancelled or diverted

			return true;
		}

	}

	/**
	 * Reduce class to apply equi-join on the flight Mapper output data
	 */
	public static class AirportPerformanceReducer extends Reducer<AirlineOrgTextPair, Text, AirlineOrgTextPair, IntWritable> {
		

		/**
		 * Reduce call will be made for every unique key value along with the list of
		 * related records
		 */
		public void reduce(AirlineOrgTextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			IntWritable finaldelay = new IntWritable();
			int totalDelay = 0;
			int totalFligths = 0;

			for (Text value : values) {
				totalDelay += ((int) Integer.parseInt(value.toString()));
				totalFligths++;
			}

			// Append last valid data of the current flight
			int avgDelay = (int) Math.ceil(totalDelay / totalFligths);
			finaldelay.set(avgDelay);

			// Output the formatted string in the file
			context.write(key, finaldelay);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageFlighDelayByMonth <in> <out>");
			System.exit(2);
		}
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
		job.setJarByClass(AirportPerformance.class);
		job.setMapperClass(AirportPerformanceMapper.class);
		job.setReducerClass(AirportPerformanceReducer.class);
		job.setOutputKeyClass(AirlineOrgTextPair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
