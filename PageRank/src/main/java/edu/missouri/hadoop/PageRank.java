 package edu.missouri.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Page Rank example
 * 
 * Based on pseudocode from: "Data-Intensive Text Proccesing with MapReduce"
 * From: Jimmy Lin and Chris Dyer
 */
public class PageRank {

	/**
	 * -PseudoExample- class Mapper method Map(nid n, node N ) p â†�
	 * N.PageRank/|N.AdjacencyList| Emit(nid n, N ) //Pass along graph structure
	 * for all nodeid m âˆˆ N.AdjacencyList do Emit(nid m, p) //Pass PageRank
	 * mass to neighbors
	 */
	public static class Map extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// TODO: your implementation here, use the pseudo code as
			// instruction
			PRNode pRNode = new PRNode("node," + value.toString());
			double p = pRNode.getPagerank() / pRNode.adjacencyListSize();
			context.write(new IntWritable(pRNode.getNodeId()),
					new Text("node,"+pRNode.toString()));
			for (int v : pRNode.getAdjacencyList()) {
				context.write(new IntWritable(v), new Text((new PRNode("not,"
						+ p)).toString()));
			}

		}
	}

	/**
	 * -PseudoExample- class Reducer method Reduce(nid m, [p1 , p2 , . . .]) M
	 * â†�âˆ… for all p âˆˆ counts [p1 , p2 , . . .] do if IsNode(p) then M â†�p
	 * //Recover graph structure else sâ†�s+p //Sum incoming PageRank
	 * contributions M.PageRank â†� s Emit(nid m, node M )
	 */
	public static class Reduce extends
			Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			// TODO: your implementation here, use the pseudo code as
			// instruction
			PRNode M = null;
			double s = 0;
			for (Text v : values) {
				if (new PRNode(v.toString()).isNode()) {
					M = new PRNode(v.toString());
				} else {
					s = s + new PRNode(v.toString()).getPagerank();
				}
			}
			M.setPagerank(s);
			context.write(key, new DoubleWritable(M.getPagerank()));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank - Example 1");

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
