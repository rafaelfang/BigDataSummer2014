package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Object, Text, IntWritable, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String s = value.toString();
		String[] array = s.split("	");
		if (array.length!=2) {
			context.write(new IntWritable(1), new Text("errorpassword"));
		} else {
			context.write(new IntWritable(Integer.parseInt(array[1])),
					new Text(array[0]));
		}

	}
}
