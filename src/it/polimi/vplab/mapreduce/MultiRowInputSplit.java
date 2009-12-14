package it.polimi.vplab.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * This class models an Input Split composed of a set of rows.
 * @author codazzo@gmail.com
 *
 */
public class MultiRowInputSplit extends InputSplit implements Writable {

	long[] rows;
	int size;
	
	//the default constructor is necessary as input splits are built with Java's ReflectionUtils.
	MultiRowInputSplit() {
		
	}
	
	public MultiRowInputSplit(long[] rows) {
		// TODO Auto-generated constructor stub
		this.rows=rows;
		this.size = rows.length;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return rows.length;
	}

	

	public long[] getRows(){
		return rows;
	}

	// Writable methods
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.size = in.readInt();
		this.rows = new long[size];
		for(int i=0; i<size; i++){
			rows[i]=in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(size);
		for(int i=0; i<size; i++){
			out.writeLong(rows[i]);
		}
	}
	
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//return null;
		return new String[]{};
	}
}
