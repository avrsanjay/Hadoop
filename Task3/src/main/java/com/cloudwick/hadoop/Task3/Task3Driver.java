package com.cloudwick.hadoop.Task3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task3Driver extends Configured implements Tool 
{
	public static enum COUNTERS{
		C200,C302,C304,C401,C404
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Task3Driver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Task3:Status code Counter");
		job.setJarByClass(Task3Driver.class);

		job.setMapperClass(Task3Mapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		return job.waitForCompletion(true)?0:1;
	}

	private static class Task3Mapper extends Mapper<Object, Text, NullWritable, NullWritable> 
	{
		String S200 = new String("200");
		String S302 = new String("302");
		String S304 = new String("304");
		String S401 = new String("401");
		String S404 = new String("404");

		String word;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) 
			{
				word = new String(tokenizer.nextToken());

				if(word.equals(S200))
					context.getCounter(COUNTERS.C200).increment(1L);
				else if(word.toString().equals(S302))
					context.getCounter(COUNTERS.C302).increment(1L);
				else if(word.toString().equals(S304))
					context.getCounter(COUNTERS.C304).increment(1L);
				else if(word.toString().equals(S401))
					context.getCounter(COUNTERS.C401).increment(1L);
				else if(word.toString().equals(S404))
					context.getCounter(COUNTERS.C404).increment(1L);		
			}
		}
	}
}