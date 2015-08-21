package com.cloudwick.hadoop.Task5;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task5Mapper extends Mapper<LongWritable, Text, TextTriplet, IntWritable>
{
    private static TextTriplet texttriplet = new TextTriplet();
    private static IntWritable one = new IntWritable(1);
    private static String delims = " ";
 
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String tokens[] = line.split(delims);
        texttriplet.set(new Text(tokens[0]), new Text(tokens[3]+" "+tokens[4]), new Text(tokens[6]));
        context.write(texttriplet, one);
    }
}
