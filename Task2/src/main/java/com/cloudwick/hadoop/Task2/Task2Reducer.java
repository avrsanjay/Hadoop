package com.cloudwick.hadoop.Task2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	public void reduce(Text text, Iterable<IntWritable> values,	Context context)
					throws IOException, InterruptedException 
	{
		int sum = 0;
		for (Iterator<IntWritable> iterator = values.iterator(); iterator.hasNext() ; iterator.next()) {
			sum ++;
		}

		context.write(text, new IntWritable(sum));
	}
}
