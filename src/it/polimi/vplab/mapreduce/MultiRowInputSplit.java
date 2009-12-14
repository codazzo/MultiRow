package it.polimi.vplab.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This class models an Input Split composed of a set of rows.
 * @author codazzo@gmail.com
 *
 */
public class MultiRowInputSplit extends InputSplit implements Writable {

	private long[] rows; //these are the rows the split consists of
	private int size; //this is the size of the split
	
	//the default constructor is necessary as input splits are built with Java's ReflectionUtils.
	MultiRowInputSplit() {
		
	}
	
	public MultiRowInputSplit(long[] rows) {
		this.rows=rows;
		this.size = rows.length;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return rows.length;
	}

	/**
	 * Helper method, used by the RecordReader
	 * @return The array of rows stored in this split.
	 */
	public long[] getRows(){
		return rows;
	}

	// Writable methods
	@Override
	public void readFields(DataInput in) throws IOException {
		this.size = in.readInt();
		this.rows = new long[size];
		for(int i=0; i<size; i++){
			rows[i]=in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size);
		for(int i=0; i<size; i++){
			out.writeLong(rows[i]);
		}
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		//return null;
		return new String[]{};
	}
}
