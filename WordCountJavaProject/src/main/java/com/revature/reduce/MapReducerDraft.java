package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MapReducerDraft  extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static Text maxKey = null;
	private static IntWritable maxCount = new IntWritable(0);
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException{
		int wordCount = 0;
		for(IntWritable value: values){
			wordCount += value.get();
		}
		if (maxKey == null || wordCount> maxCount.get()){
			MapReducerDraft.setKey(key, new IntWritable(wordCount));
		}
		context.write(maxKey, maxCount);
	}
	
	private static void setKey(Text key, IntWritable wordCount){
		maxKey = key;
		maxCount = wordCount;
	}
}
