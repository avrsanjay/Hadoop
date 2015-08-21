package com.cloudwick.hadoop.Task6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CustomLineRecordReader extends RecordReader<LongWritable, Text>
{
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();
	@Override
	public void close() throws IOException {
		if (in != null) {
            in.close();
        }
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
	}

	@Override
	public void initialize(InputSplit genericsplit, TaskAttemptContext context)
			throws IOException, InterruptedException 
	{
		//here we get input split size for a mapper
		FileSplit split = (FileSplit) genericsplit;

		//getting the configuration properties associated with this job
		Configuration job = context.getConfiguration();

		//the following line gets the value associated with the property name specified, and if the property doesnt exist it takes the second argument
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

		//the following couple of lines we get the stats of input split, like starting byte offset and length of the split
		start = split.getStart();
		end = start + split.getLength();

		//now get the input split path in the file system and create input stream
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);

		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			// Set the file pointer at "start - 1" position.
			// This is to make sure we won't miss any line
			// It could happen if "start" is located on a EOL
			--start;
			fileIn.seek(start);
		}

		in = new LineReader(fileIn, job);

		// If first line needs to be skipped, read first line
		// and stores its content to a dummy Text
		if (skipFirstLine) {
			Text dummy = new Text();
			// Reset "start" to "start + line offset"
			start += in.readLine(dummy, 0,
					(int) Math.min(
							(long) Integer.MAX_VALUE, 
							end - start));
		}

		// Position is the actual start
		this.pos = start;
		/*----------------------------------------------------------------------------------------------------------------------------------*/
		/*
		 * if the split that we got is not the starting split and instead it is some where in the middle or end, the following are possible errors
		 * 1)split might have started been exactly after a new line character
		 * 2)split might have been exactly before a new line character
		 * 3)split might have been exactly on the new line character
		 * 4)split might have been exactly on the EOF
		 */
		//		PossibleErrorsExist = (start == 0) ? false : true;
		//		
		//		if(PossibleErrorsExist)
		//		{
		//			fileIn.seek(start);
		//			this.in = new LineReader(fileIn,job);
		//			this.in.readLine(this.dummy, 1);
		//			
		//			if(this.dummy.equals("n"))
		//			{
		//				fileIn.seek(start-1);
		//				this.in = new LineReader(fileIn,job);
		//				this.in.readLine(this.dummy, 1);
		//				if(this.dummy.equals("\\"))
		//				{
		//					this.start = this.start + 1;
		//					this.pos = this.start;
		//				}
		//			}
		//			else if(this.dummy.equals("\\"))
		//			{
		//				fileIn.seek(start+1);
		//				this.in = new LineReader(fileIn,job);
		//				this.in.readLine(this.dummy, 1);
		//				if(this.dummy.equals("n"))
		//				{
		//					this.start = this.start + 2;
		//					this.pos = this.start;
		//				}
		//			}
		//			else if(this.dummy.equals("\u001a"))
		//			{
		//					this.start = this.start - 1;
		//					this.pos = this.start;
		//			}
		//		}


	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException 
	{

        // Current offset is the key
        key.set(pos);
 
        int newSize = 0;
 
        // Make sure we get at least one record that starts in this Split
        while (pos < end) {
 
            // Read first line and store its content to "value"
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(
                            Integer.MAX_VALUE, end - pos),
                            maxLineLength));
 
            // No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
            if (newSize == 0) {
                break;
            }
 
            // Line is read, new position is set
            pos += newSize;
 
            // Line is lower than Maximum record line size
            // break and return true (found key / value)
            if (newSize < maxLineLength) {
                break;
            }
 
            // Line is too long
            // Try again with position = position + line offset,
            // i.e. ignore line and go to next one
        }
 
         
        if (newSize == 0) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        } else {
            // Tell Hadoop a new line has been found
            // key / value will be retrieved by
            // getCurrentKey getCurrentValue methods
            return true;
        }
 
	}
}