package avgflightdelay;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

public class AverageFlightDelay {
	/**
	 * Mapper class to apply projection on the flight data
	 */

	/**
	 * Global counter shared by all reduce tasks
	 */
	public enum HadoopCounter {
		TotalDelay, TotalFlights
	}

	public static class AverageFlighDelayMapper extends Mapper<Object, Text, AirlineOrgDestTextPair, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');

		/**
		 * Key : Byte offset from where the line is being read value : string
		 * representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			AirlineOrgDestTextPair key = new AirlineOrgDestTextPair();
			Text delayMinutes = new Text();

			if (line.length > 0 && isValidEntry(line)) {

				// Set (FlightDate,IntermediateAirPort) as key
				key.setAirLineName(line[8]);
				key.setOrigin(line[16]);
				key.setDest(line[17]);
				int[] delays = new int[7];
				delays[0] = (line[14].equals("NA") ? 0 : Integer.parseInt(line[14]));
				delays[1] = (line[15].equals("NA") ? 0 : Integer.parseInt(line[15]));
				delays[2] = (line[24].equals("NA") ? 0 : Integer.parseInt(line[24]));
				delays[3] = (line[26].equals("NA") ? 0 : Integer.parseInt(line[25]));
				delays[4] = (line[27].equals("NA") ? 0 : Integer.parseInt(line[26]));
				delays[5] = (line[28].equals("NA") ? 0 : Integer.parseInt(line[27]));

				int sum = 0;
				for (int i = 0; i < delays.length; i++)
					sum += delays[i];
				// set value as arrDelayMinutes
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
			if (record[0].isEmpty() || record[2].isEmpty() || record[8].isEmpty() || record[16].isEmpty()
					|| record[17].isEmpty()) {
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
	public static class AverageFlightDelayReducer
			extends Reducer<AirlineOrgDestTextPair, Text, AirlineOrgDestTextPair, IntWritable> {

		private CSVParser csvParser = null;
		private int totalFlights;
		private float totalDelay;

		/**
		 * setup will be called once per Map Task before any of Map function call, we'll
		 * initialize hashMap here
		 */
		protected void setup(Context context) {
			this.csvParser = new CSVParser(',', '"');
			this.totalDelay = 0;
			this.totalFlights = 0;
		}

		/**
		 * Reduce call will be made for every unique key value along with the list of
		 * related records
		 */
		public void reduce(AirlineOrgDestTextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			IntWritable finaldelay = new IntWritable();
			int totalDelay = 0;
			int totalFligths = 0;
			int currWk = 1;
			StringBuilder output = new StringBuilder();

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

		/**
		 * cleanup will be called once per Map Task after all the Map function calls,
		 * we'll write hash of words to the file here
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(HadoopCounter.TotalDelay).increment((long) totalDelay);
			context.getCounter(HadoopCounter.TotalFlights).increment(totalFlights);

		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageFlighDelay <in> <out>");
			System.exit(2);
		}
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
		job.setJarByClass(AverageFlightDelay.class);
		job.setMapperClass(AverageFlighDelayMapper.class);
		job.setReducerClass(AverageFlightDelayReducer.class);
		job.setOutputKeyClass(AirlineOrgDestTextPair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
