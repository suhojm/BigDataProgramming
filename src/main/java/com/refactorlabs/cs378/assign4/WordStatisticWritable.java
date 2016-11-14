package com.refactorlabs.cs378.assign4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class WordStatisticWritable implements Writable {

	long documentCounts;
	long totalCounts;
	long sumOfSquares;
	double mean;
	double variance;
	
	public WordStatisticWritable(long l1, long l2, long l3, double d1, double d2){
		documentCounts = l1;
		totalCounts = l2;
		sumOfSquares = l3;
		mean = d1;
		variance = d2;
	}
	
	public WordStatisticWritable() {
		// TODO Auto-generated constructor stub
//		super();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(documentCounts);
		out.writeLong(totalCounts);;
		out.writeLong(sumOfSquares);;
		out.writeDouble(mean);
		out.writeDouble(variance);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		documentCounts = in.readLong();
		totalCounts = in.readLong();
		sumOfSquares = in.readLong();
		mean = in.readDouble();
		variance = in.readDouble();
	}

	public static WordStatisticWritable read(DataInput in) throws IOException {
		WordStatisticWritable result = new WordStatisticWritable();
		result.readFields(in);
		return null;
	}
	
	public Object[] get() {
		// TODO Auto-generated method stub
		Object[] result = {documentCounts, totalCounts, sumOfSquares, mean, variance};
		return result;
	}
	
	public String[] parse(String input){
		String inputStr = input;
		inputStr = inputStr.replaceAll("--", " ");
		inputStr = inputStr.replaceAll("(?<=.)(?=\\[.+\\])", " ");
		inputStr = inputStr.replaceAll("[.,:;?!\"_]", " ");
		inputStr = inputStr.toLowerCase();
		inputStr = inputStr.trim();
		
		String[] result = inputStr.split("\\s+");
		return result;
	}
	
	public boolean equals(WordStatisticWritable wsw){
		if((this.documentCounts == wsw.documentCounts) 
				&& (this.totalCounts == wsw.totalCounts) 
				&& (this.sumOfSquares == wsw.sumOfSquares)
				&& (this.mean == wsw.mean)
				&& (this.variance == wsw.variance))
			return true;
		else 
			return false;
	}
	
	public String toString(){
		StringBuilder result = new StringBuilder();
				
		result.append(String.valueOf(documentCounts));
		result.append(",");
		result.append(String.valueOf(totalCounts));
		result.append(",");
		result.append(String.valueOf(sumOfSquares));
		result.append(",");
		result.append(String.valueOf(mean));
		result.append(",");
		result.append(String.valueOf(variance));
		
		return result.toString();
	}

}
