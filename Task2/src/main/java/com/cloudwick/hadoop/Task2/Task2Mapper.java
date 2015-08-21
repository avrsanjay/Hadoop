package com.cloudwick.hadoop.Task2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudwick.hadoop.Task2.Task2Driver.COUNTERS;

public class Task2Mapper extends	Mapper<Object, Text, Text, IntWritable> 
{

	String S200 = new String("C200");
	String S302 = new String("C302");
	String S304 = new String("C304");
	String S401 = new String("C401");
	String S404 = new String("C404");

	Text word = new Text();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
	
		while (tokenizer.hasMoreTokens()) 
		{
			word.set(tokenizer.nextToken());
			
			if(word.equals(S200))
				context.getCounter(COUNTERS.C200).increment(1);
			else if(word.equals(S302))
				context.getCounter(COUNTERS.C302).increment(1);
			else if(word.equals(S304))
				context.getCounter(COUNTERS.C304).increment(1);
			else if(word.equals(S401))
				context.getCounter(COUNTERS.C401).increment(1);
			else if(word.equals(S404))
				context.getCounter(COUNTERS.C404).increment(1);		
		}
	}
}