package it.polimi.vplab.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This class extends RecordReader to feed key/value pairs to the Mapper.
 * In our scenario we are only interested in keys.
 * 
 * @author codazzo@gmail.com
 *
 */
public class MultiRowRecordReader extends RecordReader<LongWritable, Text> {
	int pos;
	long[] rows;

	public MultiRowRecordReader(MultiRowInputSplit hbis){
		this.pos=-1;
		this.rows=hbis.getRows();
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(rows[pos]);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// we don't really use values for this input format
		return new Text();
		//return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return pos/rows.length;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		//do we need anything here?
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		pos++;
		if(pos<rows.length){
			return true;
		}else{
			return false;
		}

	}

}
