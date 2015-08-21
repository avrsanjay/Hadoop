package com.cloudwick.hadoop.Task5;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
	MapReduceDriver<LongWritable, Text, TextTriplet, IntWritable, Text, IntWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, TextTriplet, IntWritable> mapDriver;
	ReduceDriver<TextTriplet, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		Task5Mapper mapper = new Task5Mapper();
		Task5Reducer reducer = new Task5Reducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
		mapDriver.withInput(new LongWritable(2), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
		mapDriver.withInput(new LongWritable(3), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
		mapDriver.withInput(new LongWritable(4), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));

		mapDriver.withOutput(new TextTriplet("lj1089.inktomisearch.com","[08/Mar/2004:01:04:54 -0800]","/twiki/bin/oops/TWiki/InterWikis"), new IntWritable(1));
		mapDriver.withOutput(new TextTriplet("lj1089.inktomisearch.com","[08/Mar/2004:01:04:54 -0800]","/twiki/bin/oops/TWiki/InterWikis"), new IntWritable(1));
		mapDriver.withOutput(new TextTriplet("lj1089.inktomisearch.com","[08/Mar/2004:01:04:54 -0800]","/twiki/bin/oops/TWiki/InterWikis"), new IntWritable(1));
		mapDriver.withOutput(new TextTriplet("lj1089.inktomisearch.com","[08/Mar/2004:01:04:54 -0800]","/twiki/bin/oops/TWiki/InterWikis"), new IntWritable(1));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		TextTriplet texttriplet = new TextTriplet("lj1089.inktomisearch.com","[08/Mar/2004:01:04:54 -0800]","/twiki/bin/oops/TWiki/InterWikis");
		reduceDriver.withInput(texttriplet, values);
		reduceDriver.withOutput(new Text("lj1089.inktomisearch.com [08/Mar/2004:01:04:54 -0800] /twiki/bin/oops/TWiki/InterWikis"), new IntWritable(4));
		reduceDriver.runTest();
	}
	
  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(1), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
    mapReduceDriver.withInput(new LongWritable(2), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
    mapReduceDriver.withInput(new LongWritable(3), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
    mapReduceDriver.withInput(new LongWritable(4), new Text("lj1089.inktomisearch.com - - [08/Mar/2004:01:04:54 -0800] \"GET /twiki/bin/oops/TWiki/InterWikis HTTP/1.0\" 200 209"));
	
    mapReduceDriver.addOutput(new Text("lj1089.inktomisearch.com [08/Mar/2004:01:04:54 -0800] /twiki/bin/oops/TWiki/InterWikis"), new IntWritable(4));
    mapReduceDriver.runTest();
  }
}