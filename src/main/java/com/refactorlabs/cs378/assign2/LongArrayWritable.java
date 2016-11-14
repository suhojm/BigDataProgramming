package com.refactorlabs.cs378.assign2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class LongArrayWritable extends ArrayWritable {
	
	public LongArrayWritable() {
		 super(LongWritable.class);
	} 
	
	public String toString(){
		Writable[] wValues = get();
		String result = ((LongWritable)wValues[0]).get() + "," + ((LongWritable)wValues[1]).get() +
				"," + ((LongWritable)wValues[2]).get();
		
		return result;
	}
	
	// getter and setter for array
	public long[] getValueArray() {
		Writable[] wValues = get();
		long[] values = new long[wValues.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ((LongWritable)wValues[i]).get();
		}
		return values;
	} 
	
}
