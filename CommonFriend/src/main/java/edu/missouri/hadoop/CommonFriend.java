package edu.missouri.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 * 
 */
public class CommonFriend {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] s = value.toString().split(":");

			String myKey = s[0];
			String outputKey;
			String myValue = s[1];
			String[] array = myValue.split(" ");
			for (int i = 0; i < array.length; i++) {
				if (myKey.compareTo(array[i]) < 0) {
					outputKey = myKey + array[i];
				} else {
					outputKey = array[i] + myKey;
				}
				context.write(new Text(outputKey), new Text(myValue));
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String commonString = "";
			for (Text v : values) {
				if(commonString.isEmpty()){
					commonString=v.toString();
					continue;
				}
				commonString = getCommonString(commonString, v.toString());

			}

			context.write(key, new Text(commonString));

		}
	}

	private static String getCommonString(String stringA, String stringB) {
		
		String[] A = stringA.split(" ");
		List<String> listA = new ArrayList<String>();
		for (String s : A) {
			listA.add(s);
		}
		String[] B = stringB.split(" ");
		List<String> listB = new ArrayList<String>();
		for (String s : B) {
			listB.add(s);
		}
		List<String> common = new ArrayList<String>();

		for (String b : listB) {
			if (listA.contains(b)) {
				common.add(b);
			}
		}
		return common.toString();
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());

		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(CommonFriend.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
