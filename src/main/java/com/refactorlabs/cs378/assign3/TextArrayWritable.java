package com.refactorlabs.cs378.assign3;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable{
	public TextArrayWritable(){
		super(Text.class);
	}
	
	public String toString(){
		Writable[] wvals = get();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < wvals.length; i++){
			if(i != 0){
				sb.append(",");
			}
			sb.append(wvals[i].toString());
		}
		
		return sb.toString();
	}
}
