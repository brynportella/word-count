package com.revature.reduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
	//Old way Atomicity 
	public static volatile String CURRENT_MAX_WORD = null; 
	
	//New way Java 8 
	public static AtomicInteger CURRENT_MAX_COUNT = new AtomicInteger(Integer.MIN_VALUE);
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException{
		for (IntWritable value : values){
			CURRENT_MAX_COUNT = (value.get() > CURRENT_MAX_COUNT.get())? new AtomicInteger(value.get()):CURRENT_MAX_COUNT;
			CURRENT_MAX_WORD = (value.get() == CURRENT_MAX_COUNT.get())? key.toString():CURRENT_MAX_WORD;
		}
		
		context.write(new Text(CURRENT_MAX_WORD), new IntWritable(CURRENT_MAX_COUNT.get()));
	}
	
}
