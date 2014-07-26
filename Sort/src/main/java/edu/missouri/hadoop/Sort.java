package edu.missouri.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Sort {
	public static class SortMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(new IntWritable(Integer.parseInt(value.toString())),
					new Text(""));

		}
	}

	public static class SortReducer extends
			Reducer<IntWritable, Text, IntWritable, IntWritable> {
		int index = 1;

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text t : values) {
				context.write(new IntWritable(index), key);
				index++;
			}

		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);

		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(Sort.class);

		// job.submit();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
