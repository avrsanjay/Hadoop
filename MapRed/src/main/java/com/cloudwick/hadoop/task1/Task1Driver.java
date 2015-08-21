package com.cloudwick.hadoop.task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1Driver {

	static int map_tasks = 1;
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "task1");
			job.setJarByClass(Task1Driver.class);
			job.setMapperClass(Task1Mapper.class);
			job.setReducerClass(Task1Reducer.class);
			job.setMapOutputKeyClass(Integer.class);
			job.setMapOutputValueClass(Integer.class);
			job.setOutputKeyClass(Integer.class);
			job.setOutputValueClass(Integer.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			if(Task1Reducer.Total_words != 0)
				System.out.println("1average letters: "+Task1Reducer.Total_letters/Task1Reducer.Total_words);
			System.exit(job.waitForCompletion(true)?0:1);
			if(Task1Reducer.Total_words != 0)
				System.out.println("2average letters: "+Task1Reducer.Total_letters/Task1Reducer.Total_words);
		}

}
