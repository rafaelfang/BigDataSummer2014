package edu.missouri.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String s = value.toString();
		String[] array = s.split(" # ");
		context.write(new Text(array[1]), new IntWritable(1));
	}
}
