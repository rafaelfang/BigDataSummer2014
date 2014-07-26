package edu.missouri.hadoop;

/*
 * Big Data Summer Experience
 */

import java.lang.Exception;
import java.lang.Integer;
import java.math.BigDecimal;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.Iterable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PI extends Configured implements Tool {
	static private final Path TMP_DIR = new Path("pitmp");
	static final Log LOG = LogFactory.getLog(PI.class);

	// This is the Mapper
	public static class PiMapper extends
			Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
		public void map(LongWritable offset, LongWritable size, Context context)
				throws IOException, InterruptedException {
			final HaltonSequence hs = new HaltonSequence(offset.get());
			long nInside = 0;
			long nOutside = 0;
			String s = size.toString();
			for (int i = 0; i < Long.parseLong(s); i++) {
				// 1. generate points using hs.getNext() generate n=size
				double[] p = hs.nextPoint();
				// 2. for each point in/out
				if (p[0] * p[0] + p[1] * p[1] > 1) {
					nOutside++;
				} else  {
					nInside++;
				}
			}

			// 3. emit key=1 totalInside; key=2 totalOutside
			context.write(new LongWritable(1), new LongWritable(nInside));

			context.write(new LongWritable(2), new LongWritable(nOutside));

		}
	}

	// This is the Reducer
	public static class PiReducer extends
			Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		long nInside = 0;
		long nOutside = 0;

		public void reduce(LongWritable isInside,
				Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			// get the sum of totalinsize
			// TODO: Your implementation here
			for(LongWritable v:values){
				if (isInside.equals(new LongWritable(1)) ) {
					nInside = nInside +v.get();
				} else if (isInside.equals(new LongWritable(2))) {
					nOutside = nOutside + v.get();
				}
			}
			LOG.info("reduce-log:" + "isInside = " + isInside.get()
					+ ", nInside = " + nInside + ", nOutSide = " + nOutside);
		}

		// This cleanup function will be executed before Reducer class ends, so
		// we
		// write nInside and nOutSide values into files here.
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Path OutDir = new Path(TMP_DIR, "out");
			Path outFile = new Path(OutDir, "reduce-out");
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			@SuppressWarnings("deprecation")
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
					outFile, LongWritable.class, LongWritable.class,
					CompressionType.NONE);
			writer.append(new LongWritable(nInside), new LongWritable(nOutside));
			writer.close();
		}
	}

	public static BigDecimal estimate(int nMaps, int nSamples, Job job)
			throws Exception {
		LOG.info("\n\n estimate \n\n");

		job.setJarByClass(PI.class);
		job.setMapperClass(PiMapper.class);
		job.setReducerClass(PiReducer.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setSpeculativeExecution(false);

		Path inDir = new Path(TMP_DIR, "in");
		Path outDir = new Path(TMP_DIR, "out");

		FileInputFormat.addInputPath(job, inDir);
		FileOutputFormat.setOutputPath(job, outDir);

		// Check the directory
		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(TMP_DIR)) {
			throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR)
					+ " already exists, pls remove it.");
		}

		// Create input directory
		if (!fs.mkdirs(inDir)) {
			throw new IOException("Cannot create input directory " + inDir);
		}

		try {
			// generate several sequence files.
			for (int i = 0; i < nMaps; i++) {
				final Path file = new Path(inDir, "part" + i);
				final LongWritable offset = new LongWritable(i * nSamples);
				final LongWritable size = new LongWritable(nSamples);
				@SuppressWarnings("deprecation")
				final SequenceFile.Writer writer = SequenceFile.createWriter(
						fs, job.getConfiguration(), file, LongWritable.class,
						LongWritable.class, CompressionType.NONE);
				writer.append(offset, size);
				writer.close();
				System.out.println("wrote input for Map #" + i);
			}

			// execute MapReduce
			System.out.println("starting mapreduce job");
			final long startTime = System.currentTimeMillis();
			boolean ret = job.waitForCompletion(true);
			final double duration = (System.currentTimeMillis() - startTime) / 1000.0;
			System.out.println("Job finished in " + duration + " seconds.");

			// Read the results from HDFS
			Path inFile = new Path(outDir, "reduce-out");
			LongWritable nInside = new LongWritable();
			LongWritable nOutside = new LongWritable();
			@SuppressWarnings("deprecation")
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile,
					job.getConfiguration());
			reader.next(nInside, nOutside);
			reader.close();
			LOG.info("estimate-log: " + "nInside = " + nInside.get()
					+ ", nOutSide = " + nOutside.get());

			// return the pi value
			return BigDecimal
					.valueOf(4)
					.multiply(BigDecimal.valueOf(nInside.get()))
					.divide(BigDecimal.valueOf(nInside.get() + nOutside.get()),
							20, BigDecimal.ROUND_HALF_DOWN);
		} finally {
			fs.delete(TMP_DIR, true);
		}
	}

	public int run(String[] args) throws Exception {
		LOG.info("\n\n run \n\n");
		if (args.length != 2) {
			System.err
					.println("Use: NewPieEst <num of map task> <points per mapper>");
			System.exit(1);
		}

		// parse arguments
		int nMaps = Integer.parseInt(args[0]);
		int nSamples = Integer.parseInt(args[1]);
		Configuration conf = new Configuration();
		Job job = Job.getInstance((conf));
		job.setJobName("Pi estimating job");

		System.out.println("Pi = " + estimate(nMaps, nSamples, job));

		return 0;
	}

	public static void main(String[] argv) throws Exception {
		LOG.info("\n\n main \n\n");
		System.exit(ToolRunner.run(null, new PI(), argv));
	}
}