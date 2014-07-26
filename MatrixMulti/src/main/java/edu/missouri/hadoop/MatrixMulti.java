package edu.missouri.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMulti {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int m = Integer.parseInt(conf.get("m"));
			int p = Integer.parseInt(conf.get("p"));
			String line = value.toString();
			String[] indicesAndValue = line.split(",");
			Text outputKey = new Text();
			Text outputValue = new Text();

			// TODO: your implementation here
			int i, j, k;
			float a_ij, b_jk;
			if (indicesAndValue[0].equals("A")) {
				i = Integer.parseInt(indicesAndValue[1]);
				j = Integer.parseInt(indicesAndValue[2]);
				a_ij = Float.parseFloat(indicesAndValue[3]);
				for (k = 0; k < p; k++) {
					outputKey = new Text(i + "," + k);
					outputValue = new Text("A," + j + "," + a_ij);
					context.write(outputKey, outputValue);
				}
			} else {
				j = Integer.parseInt(indicesAndValue[1]);
				k = Integer.parseInt(indicesAndValue[2]);
				b_jk = Float.parseFloat(indicesAndValue[3]);
				for (i = 0; i < m; i++) {
					outputKey = new Text(i + "," + k);
					outputValue = new Text("B," + j + "," + b_jk);
					context.write(outputKey, outputValue);
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] value;
			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
			int n = Integer.parseInt(context.getConfiguration().get("n"));
			float result = 0.0f;
			float a_ij;
			float b_jk;
			//==============You code here====================
			for (Text v : values) {
				value = v.toString().split(",");
				if (value[0].equals("A")) {
					a_ij = Float.parseFloat(value[2]);
					hashA.put(Integer.parseInt(value[1]), a_ij);
				} else {
					b_jk = Float.parseFloat(value[2]);
					hashB.put(Integer.parseInt(value[1]), b_jk);
				}
			}
			// TODO: your implementation here
			for (int j = 0; j < n; j++) {
				if(hashA.containsKey(j)&&hashB.containsKey(j)){
					result += hashA.get(j) * hashB.get(j);
				}
			}
			context.write(key, new Text(result + ""));
			//==========================================
			/*if (result != 0.0f) {
				context.write(null,
						new Text(key.toString() + "," + Float.toString(result)));
			}*/
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Use: MatrixMulti <input> <output> <m> <n> <p>");
			System.exit(1);
		}
		Configuration conf = new Configuration();

		// A is an m-by-n matrix; B is an n-by-p matrix.
		conf.set("m", args[2]);
		conf.set("n", args[3]);
		conf.set("p", args[4]);

		Job job = Job.getInstance(conf);
		job.setJobName("MatrixMatrixMultiplicationOneStep");
		job.setJarByClass(MatrixMulti.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}