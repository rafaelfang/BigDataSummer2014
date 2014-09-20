package edu.missouri.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class App {
	private static final String message = "My First Hadoop API call!";

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path filenamePath = new Path("hadoop.txt");
		if (fs.exists(filenamePath)) {
			fs.delete(filenamePath, true);
		}

		FSDataOutputStream out = fs.create(filenamePath);
		out.writeUTF(message);
		out.close();

		FSDataInputStream in = fs.open(filenamePath);
		String messageIn = in.readUTF();
		System.out.println(messageIn);
		in.close();
	}
}
