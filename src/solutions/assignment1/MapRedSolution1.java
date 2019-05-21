package solutions.assignment1;

import java.io.IOException;
import java.util.StringTokenizer;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution1 {
	/* your code goes in here */
	public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			// itr.nextToken();
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				// if (str.length() == 0) continue;
				// str.charAt(0);
				if (str.startsWith("\"") == false)
					continue;
				// if (str.equals("\"GET") == false) continue;
				// if (str.startsWith("\"") == false) continue;
				if (itr.hasMoreTokens()) {
					str = itr.nextToken();
					if (itr.hasMoreTokens()) {
						String str1 = itr.nextToken();
						// StringTokenizer itr1 = new StringTokenizer(str1, "/");
						/*
						 * if (itr1.hasMoreTokens()) { String str2 = itr1.nextToken(); if
						 * (str2.equals("HTTP") == false) continue;
						 */
						if (str1.startsWith("HTTP")) {
							String str3 = "http://www.example.com" + str;
							word.set(str3);
							context.write(word, one);
						}else if(str.equals("/\"")) {
						word.set("http://www.example.com/");
						context.write(word, one);
					}

					}
				}
			}
		}

	}

	public static class ReduceRecords extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable val : values)
				sum += val.get();

			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: MapRedSolution1 <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "MapRed Solution #1");

		job.setMapperClass(MapRecords.class);
		job.setCombinerClass(ReduceRecords.class);
		job.setReducerClass(ReduceRecords.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		/* your code goes in here */

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		MapRedFileUtils.deleteDir(otherArgs[1]);
		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1] + "/part-r-00000"));
		String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
		fileInputStream.close();

		String[] validMd5Sums = { "ca11be10a928d07204702b3f950fb353", "6a70a6176249b0f16bdaeee5996f74cb",
				"54893b270934b63a25cd0dcfd42fba64", "d947988bd6f35078131ce64db48dfad2",
				"3c3ded703f60e117d48c3c37e2830866" };

		for (String validMd5 : validMd5Sums) {
			if (validMd5.contentEquals(md5)) {
				System.out.println("The result looks good :-)");
				System.exit(exitCode);
			}
		}
		System.out.println("The result does not look like what we expected :-(");
		System.exit(exitCode);
	}
}
