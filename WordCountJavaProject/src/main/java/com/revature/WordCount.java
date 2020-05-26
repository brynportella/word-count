package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WordMapper;
import com.revature.reduce.MaxReducer;
import com.revature.reduce.SumReducer;

public class WordCount {
	public static void main(String[] args) throws Exception{
		if (args.length != 2){
			System.out.printf("Usage of Word Count <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job();
		
		job.setJarByClass(WordCount.class);
		
		job.setJobName("Word Count");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WordMapper.class);
		
		job.setCombinerClass(SumReducer.class);
		
		job.setReducerClass(MaxReducer.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		
		System.exit(success? 0: 1);
	}
}
