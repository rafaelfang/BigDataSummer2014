package com.hackecho.hadoop.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * first implementation using hashmap
 */
public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

	Map<String, Integer> map;

	public void setup(Context context) throws IOException, InterruptedException {
		map = new HashMap<String, Integer>();
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer wordList = new StringTokenizer(value.toString());

		while (wordList.hasMoreTokens()) {
			String w = wordList.nextToken();
			if (map.containsKey(w)) {
				map.put(w, map.get(w) + 1);
			} else {
				map.put(w, 1);
			}
		}

	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<String, Integer> entry : map.entrySet()) {
			context.write(new Text(entry.getKey()),
					new IntWritable(entry.getValue()));
		}
	}
}
