package com.cloudwick.hadoop.Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2Driver extends Configured implements Tool 
{

	public enum COUNTERS{
		C200,C302,C304,C401,C404
	}
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Task2Driver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception 
	{

		// When implementing tool
		Configuration conf = this.getConf();

		// Create job
		Job job = new Job(conf, "Task2 : Status Code Count");
		job.setJarByClass(Task2Driver.class);
	
		// Setup MapReduce job
		// Do not specify the number of Reducer
		job.setMapperClass(Task2Mapper.class);
		//job.setReducerClass(Task2Reducer.class);

		// Specify key / value
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job and return status
		boolean i = job.waitForCompletion(false);
		
		Counters counters = job.getCounters();
		System.out.println("C200"+counters.findCounter(COUNTERS.C200));
		System.out.println("C302"+counters.findCounter(COUNTERS.C302));
		System.out.println("C304"+counters.findCounter(COUNTERS.C304));
		System.out.println("C401"+counters.findCounter(COUNTERS.C401));
		System.out.println("C404"+counters.findCounter(COUNTERS.C404));
		
		return i?0:1;
	}
}
