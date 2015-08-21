package com.cloudwick.hadoop.Task5;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Task5Driver extends Configured implements Tool{
	
	static final Logger logger = Logger.getLogger(Task5Driver.class);  

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Task5Driver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("Task 5");
		
		job.setJarByClass(Task5Driver.class);
		
		job.setMapperClass(Task5Mapper.class);
		job.setMapOutputKeyClass(TextTriplet.class);
        job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(Task5Reducer.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		
        FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true)?0:1;
	}

}
