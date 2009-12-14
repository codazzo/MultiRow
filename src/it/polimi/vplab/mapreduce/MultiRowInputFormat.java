package it.polimi.vplab.mapreduce;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is an Input Format for mappers which need to process a set of rows
 * each represented by a <code>long</code>.
 * Example use:
 * 
 * <code><br>
 * ...<br>
		job.setInputFormatClass(HBaseInputFormat.class);<br>
		job.setMapOutputKeyClass(LongWritable.class);<br>
		job.setMapOutputValueClass(Text.class);<br>
<br>
		HBaseInputFormat.setValues(rows);<br>
		HBaseInputFormat.setSplitSize(100);<br>
		...<br>
		res = job.waitForCompletion(true);<br>
	</code>
 * @author codazzo@gmail.com
 *
 */
public class MultiRowInputFormat extends InputFormat<LongWritable, Text>{
	static int size=100; //default value: you should always change this
	static long[] rows;

	public static void setValues(long[] myRows){
		rows = myRows;
	}


	/**
	 * Call this method before submitting the job to set the size of each input split.
	 * 
	 * @param mySize The size (number of rows) each Input Split should have.
	 */
	public static void setSplitSize(int mySize){
		size=mySize;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader( InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new MultiRowRecordReader((MultiRowInputSplit) split);
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
		ArrayList<InputSplit> res = new ArrayList<InputSplit>();

		for(int i=0; i<rows.length; i=i+size){
			int newSize = Math.min(size, (Math.min(rows.length, i+size)-i)); //this... is actually right

			long[] tempRows = new long[newSize];
			for(int j=i; j<Math.min(rows.length, i+size); j++){
				tempRows[j%size]=rows[j];
			}

			res.add(new MultiRowInputSplit(tempRows));
		}
		return res;
	}


}
