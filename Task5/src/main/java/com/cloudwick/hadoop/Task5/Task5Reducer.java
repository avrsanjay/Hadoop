package com.cloudwick.hadoop.Task5;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task5Reducer extends Reducer<TextTriplet, IntWritable, Text, IntWritable>
{    
    @Override
    public void reduce(TextTriplet key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int count=0;
        
        for(IntWritable value: values)
        {
            count += value.get();
        }
        context.write(new Text(key.getIp()+" "+key.getReqPage()+" "+key.getTimeStamp()), new IntWritable(count));
    }
}
