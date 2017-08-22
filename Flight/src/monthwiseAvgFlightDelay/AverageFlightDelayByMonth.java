package monthwiseAvgFlightDelay;

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

public class AverageFlightDelayByMonth {
	public static class AverageFlightDelayByMonthMapper extends Mapper<Object, Text, AirlineTextPair, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		private String getMonthName(String month) {
			int m= Integer.parseInt(month);
			switch(m) {
			case 1: return "January"; 
			case 2:return "February";
			case 3:return "March";
			case 4:return "April";
			case 5:return "May";
			case 6:return "June";
			case 7:return "July";
			case 8:return "August";
			case 9:return "September";
			case 10:return "October";
			case 11:return "November";
			case 12:return "December";
			default: return "";
			}		
		}
		/**
		 * Key : Byte offset from where the line is being read value : string
		 * representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			AirlineTextPair key = new AirlineTextPair();
			Text delayMinutes = new Text();

			if (line.length > 0 && isValidEntry(line)) {

				// Set (FlightDate,IntermediateAirPort) as key
				key.setAirLineName(line[8]);
				key.setMonth(getMonthName(line[1].trim()));
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
	public static class AverageFlightDelayByMonthReducer extends Reducer<AirlineTextPair, Text, AirlineTextPair, IntWritable> {

		/**
		 * Reduce call will be made for every unique key value along with the list of
		 * related records
		 */
		public void reduce(AirlineTextPair key, Iterable<Text> values, Context context)
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
		job.setJarByClass(AverageFlightDelayByMonth.class);
		job.setMapperClass(AverageFlightDelayByMonthMapper.class);
		job.setReducerClass(AverageFlightDelayByMonthReducer.class);
		job.setOutputKeyClass(AirlineTextPair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
