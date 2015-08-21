package com.cloudwick.hadoop.task1;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer
extends Reducer<Integer,Integer,Integer,Integer> 
{
	static Integer Total_words = new Integer(0);
	static Integer Total_letters = new Integer(0);
	
	public void reduce(Integer number_of_words, Integer letter_count, Context context) throws IOException, InterruptedException 
	{
		Total_words+=number_of_words;
		Total_letters+=letter_count;
		//context.write(Total_words, Total_letters);
		//System.out.println("total words:"+Total_words+" total letters:"+Total_letters);
		//context.write(Total_words.get(), Total_letters.get());
	}
	@SuppressWarnings("unchecked")
	protected void cleanup(@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException{
		context.write(Total_words, Total_letters);
	}
	
}