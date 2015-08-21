package com.cloudwick.hadoop.Task6;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class Task6Driver extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Task6Driver(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		// When implementing tool
        Configuration conf = this.getConf();		
        Job job = Job.getInstance(conf);
		job.setJobName("Task 5");
		
		job.setJarByClass(Task6Driver.class);
		
		job.setMapperClass(Task6Mapper.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(Task6Reducer.class);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		
        FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(CustomFileInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true)?0:1;
	}

}
