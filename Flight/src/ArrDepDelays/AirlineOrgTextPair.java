package ArrDepDelays;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * AirlineTextPair implements the WritableComparable This class contains useful
 * functions used for keyComparator and Grouping comparator
 */
public class AirlineOrgTextPair implements WritableComparable {
	// Airline's UniqueCarrier
	Text airLineName;
	// Airlines fly Month
	Text origin;

	/**
	 * constructor
	 */
	public AirlineOrgTextPair() {
		this.airLineName = new Text();
		this.origin = new Text();
	}

	/**
	 * constructor
	 * 
	 * @param airLineName
	 * @param month
	 */
	public AirlineOrgTextPair(Text airLineName, Text origin) {
		this.airLineName = airLineName;
		this.origin = origin;
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
	public void setOrigin(String origin) {
		this.origin.set(origin.getBytes());
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
	public Text getOrigin() {
		return this.origin;
	}

	/**
	 * overrider the write method to support write operation
	 */
	public void write(DataOutput out) throws IOException {
		this.airLineName.write(out);
		this.origin.write(out);
	}

	/**
	 * overrider readFiled method to support reading fields
	 */
	public void readFields(DataInput in) throws IOException {
		if (this.airLineName == null)
			this.airLineName = new Text();

		if (this.origin == null)
			this.origin = new Text();

		this.airLineName.readFields(in);
		this.origin.readFields(in);
	}

	/**
	 * Sort first by airline name and then by month in increasing order
	 */
	public int compareTo(Object object) {
		AirlineOrgTextPair ip2 = (AirlineOrgTextPair) object;
		int cmp = getAirLineName().compareTo(ip2.getAirLineName());
		if (cmp != 0) {
			return cmp;
		}
		cmp = getOrigin().compareTo(ip2.getOrigin());
		return cmp;
	}

	/**
	 * provide comparator for airline name for grouping comparator
	 */
	public int compare(Object object) {
		AirlineOrgTextPair airline2 = (AirlineOrgTextPair) object;
		return getAirLineName().compareTo(airline2.getAirLineName());
	}

	/**
	 * use this hashcode for partitioning
	 */
	public int hashCode() {
		return this.airLineName.hashCode();
	}

	public boolean equals(Object o) {
		AirlineOrgTextPair p = (AirlineOrgTextPair) o;
		return (this.airLineName.equals(p.getAirLineName()) && (this.origin.equals(p.getOrigin())));
	}

	public Text toText() {
		return new Text( this.airLineName.toString() + "," + this.origin.toString());
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(airLineName).append(",").append(origin)).toString();
	}
}
