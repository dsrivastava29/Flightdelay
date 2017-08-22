package yearwiseAvgFlightDelay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * AirlineTextPair implements the WritableComparable This class contains useful
 * functions used for keyComparator and Grouping comparator
 */
public class AirlineTextPair implements WritableComparable {
	// Airline's UniqueCarrier
		Text airLineName;
		// Airlines fly Month
		Text year;

		/**
		 * constructor
		 */
		public AirlineTextPair() {
			this.airLineName = new Text();
			this.year = new Text();
		}

		/**
		 * constructor
		 * 
		 * @param airLineName
		 * @param month
		 */
		public AirlineTextPair(Text airLineName, Text week) {
			this.airLineName = airLineName;
			this.year = week;
		}

		/**
		 * set airline's unique carrier name
		 * 
		 * @param airLineName
		 */
		public void setAirLineName(String airLineName) {
			this.airLineName.set(airLineName.getBytes());
		}

		/**
		 * set the month of the flight fly
		 * 
		 * @param month
		 */
		public void setYear(String year) {
			this.year.set(year.getBytes());
		}

		/**
		 * get airline's unique carrier name
		 */
		public Text getAirLineName() {
			return this.airLineName;
		}

		/**
		 * get the month of the flight fly
		 */
		public Text getYear() {
			return this.year;
		}

		/**
		 * overrider the write method to support write operation
		 */
		public void write(DataOutput out) throws IOException {
			this.airLineName.write(out);
			this.year.write(out);
		}

		/**
		 * overrider readFiled method to support reading fields
		 */
		public void readFields(DataInput in) throws IOException {
			if (this.airLineName == null)
				this.airLineName = new Text();

			if (this.year == null)
				this.year = new Text();

			this.airLineName.readFields(in);
			this.year.readFields(in);
		}

		/**
		 * Sort first by airline name and then by month in increasing order
		 */
		public int compareTo(Object object) {
			AirlineTextPair ip2 = (AirlineTextPair) object;
			int cmp = getAirLineName().compareTo(ip2.getAirLineName());
			if (cmp != 0) {
				return cmp;
			}
			return getYear().compareTo(ip2.getYear());		
		}

		/**
		 * provide comparator for airline name for grouping comparator
		 */
		public int compare(Object object) {
			AirlineTextPair airline2 = (AirlineTextPair) object;
			return getAirLineName().compareTo(airline2.getAirLineName());
		}

		/**
		 * use this hashcode for partitioning
		 */
		public int hashCode() {
			return this.airLineName.hashCode();
		}

		public boolean equals(Object o) {
			AirlineTextPair p = (AirlineTextPair) o;
			return (this.airLineName.equals(p.getAirLineName()) && (this.year.equals(p.getYear())));
		}

		public Text toText() {
			return new Text( this.airLineName.toString() + "," + this.year.toString());
		}
		
		@Override
		public String toString() {
			return (new StringBuilder().append(airLineName).append(",").append(year)).toString();
		}
	}
