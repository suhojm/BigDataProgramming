package com.refactorlabs.cs378.assign2;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable(){
		super(DoubleWritable.class);
	}	
	
	public String toString(){
		Writable[] wValues = get();
		String result = ((DoubleWritable)wValues[0]).get() + "," + ((DoubleWritable)wValues[1]).get() +
				"," + ((DoubleWritable)wValues[2]).get();
		
		return result;
	}
	
	// getter and setter for array
	public double[] getValueArray() {
		Writable[] wValues = get();
		double[] values = new double[wValues.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = ((DoubleWritable)wValues[i]).get();
		}
		return values;
	} 

}
