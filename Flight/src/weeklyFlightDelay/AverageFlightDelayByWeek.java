package weeklyFlightDelay;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

import monthwiseAvgFlightDelay.AirlineTextPair;

public class AverageFlightDelayByWeek {
	public static class AverageFlightDelayByWeekMapper extends Mapper<Object, Text, AirlineTextPair, Text> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',', '"');
		private String getWeekName(String week) {
			int m= Integer.parseInt(week);
			switch(m) {
			case 1: return "Monday"; 
			case 2:return "Tuesday";
			case 3:return "Wednesday";
			case 4:return "Thursday";
			case 5:return "Friday";
			case 6:return "Saturday";
			case 7:return "Sunday";			
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
				// set key as AirlineTextPair
				key.setAirLineName(line[8]);
				key.setMonth(getWeekName(line[3].trim()));

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
		 * @param record
		 *            : line record representing flight details
		 * @return true iff entry contain all required data fields false otherwise
		 */
		private boolean isValidEntry(String[] record) {

			if (record == null || record.length == 0) {
				return false;
			}
			if (record[0].equals("Year"))
				return false;
			// If any of required field is missing, we'll ignore the record
			if (record[0].isEmpty() || record[3].isEmpty() || record[15].isEmpty() || record[14].isEmpty()
					|| record[24].isEmpty() || record[25].isEmpty() || record[26].isEmpty() || record[27].isEmpty()
					|| record[28].isEmpty()) {
				return false;
			}

			// If flight was cancelled
			if (record[22].equals("1")) {
				return false;
			}

			return true;
		}
	}

	/**
	 * Reduce class
	 */
	public static class AverageFlightDelayByWeekReducer extends Reducer<AirlineTextPair, Text, AirlineTextPair, IntWritable> {

		/**
		 * Reduce function will be called per Airline's Carrier Name and months will be
		 * in increasing sorted order Key : AirlineTextPair which implements
		 * WritableComparable values : sorted delay in increasing order by month for
		 * given key airline
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
	 * AirlinePartitioner will partition data based on the hashcode of the Airline's
	 * UniqueCarrier
	 */
	public static class AirlinePartitioner extends Partitioner<AirlineTextPair, Text> {
		/**
		 * Based on the configured number of reducer, this will partition the data
		 * approximately evenly based on number of unique carrier names
		 */
		@Override
		public int getPartition(AirlineTextPair key, Text value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	/**
	 * KeyComparator first sorts based on the unique carrier name and then sorts
	 * months in increasing order using compareTo method of the AirlineTextPair
	 * class
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(AirlineTextPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AirlineTextPair airline1 = (AirlineTextPair) w1;
			AirlineTextPair airline2 = (AirlineTextPair) w2;
			return airline1.compareTo(airline2);
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
		
		Job job = new Job(conf, "Average monthly flight dealy");
		job.setJarByClass(AverageFlightDelayByWeek.class);
		job.setMapperClass(AverageFlightDelayByWeekMapper.class);
		job.setReducerClass(AverageFlightDelayByWeekReducer.class);
		job.setOutputKeyClass(AirlineTextPair.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(AirlinePartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
