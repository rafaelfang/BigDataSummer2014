package edu.missouri.hadoop;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopTenPswdCSDN {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(TopTenPswdCSDN.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Path tempDir = new Path("csdn-temp-"
		// + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		Path tempDir = new Path("csdn-temp");
		FileOutputFormat.setOutputPath(job, tempDir);

		if (job.waitForCompletion(true)) {

			Job job2 = Job.getInstance(new Configuration());

			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

			job2.setSortComparatorClass(IntDecreasingComparator.class);

			job2.setMapperClass(SortMapper.class);
			job2.setReducerClass(SortReducer.class);

			job2.setNumReduceTasks(1);

			FileInputFormat.setInputPaths(job2, tempDir);
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));

			//FileSystem.get(job2.getConfiguration()).deleteOnExit(tempDir);

			job2.setJarByClass(TopTenPswdCSDN.class);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}
}
