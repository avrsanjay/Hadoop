package com.cloudwick.hadoop.Task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.Iterator;

public class Task1Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Task1Driver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception 
	{

		// When implementing tool
		Configuration conf = this.getConf();

		// Create job
		Job job = new Job(conf, "Tool Job");
		job.setJarByClass(Task1Driver.class);
	
		// Setup MapReduce job
		// Do not specify the number of Reducer
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		// Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Task1Mapper extends	Mapper<Object, Text, Text, IntWritable> 
	{

		private final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] csv = value.toString().split(" ");
			for (String str : csv) {
				word.set(str);
				context.write(word, ONE);
			}
		}
	}

	public static class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		@Override
		public void reduce(Text text, Iterable<IntWritable> values,	Context context)
						throws IOException, InterruptedException 
		{
			String map_tasks = context.getConfiguration().get("mapred.map.tasks");
			int sum = 0;
			for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext() ; iterator.next()) {
				sum ++;
			}
			if(sum > 14)
				Log.info("key:"+text+" sum:"+sum+" maptasks:"+map_tasks);
			context.write(text, new IntWritable(sum/Integer.parseInt(map_tasks)));
		}
	}
}
