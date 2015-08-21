package com.cloudwick.hadoop.task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper
extends Mapper<Object, Text, Integer, Integer>{

	Integer number_of_words = new Integer(0);
	Integer letter_count =  new Integer(0);
	
	public void map(Object key, Text value, Context context	) throws IOException, InterruptedException 
	{
		number_of_words=0;
		letter_count=0;
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) 
		{
			letter_count+=itr.nextToken().length();
			number_of_words++;
		}
		context.write(number_of_words, letter_count);
	}
}