package solutions.assignment2;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;


public class MapRedSolution2 {
	public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*StringTokenizer itr = new StringTokenizer(value.toString());
			Pattern pattern = Pattern.compile("\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d\\d-\\d\\d-\\d\\d");
            Matcher matcher;

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                if (str.length() == 0) continue;
                matcher = pattern.matcher(str);
                if (matcher.find()) {
                    str = str.substring(0, 2);
                    word.set(timeFormat(str));
                    context.write(word, one);
                }*/
            
			StringTokenizer itr = new StringTokenizer(value.toString());
			itr.nextToken();
			while (itr.hasMoreTokens()) {
				String str2 = itr.nextToken();
				String str1 = itr.nextToken();
				StringTokenizer itr1 = new StringTokenizer(str2,":");
				String str = itr1.nextToken();
				//itr.nextToken();
			/*StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			for(int i=0; i < 19 ; i++) {itr.nextToken();}
			while (itr.hasMoreTokens()) {
				String str1 = itr.nextToken();
				StringTokenizer itr1 = new StringTokenizer(str1);
				itr1.nextToken();
				String str2 = itr1.nextToken();
				StringTokenizer itr2 = new StringTokenizer(str2, ":");
				String str = itr2.nextToken();
				
				/*StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
				itr.nextToken();
				while (itr.hasMoreTokens()) {
					String str0 = itr.nextToken();
					StringTokenizer itr0 = new StringTokenizer(str0, ",");
					itr0.nextToken();
					String str1 = itr0.nextToken();
					StringTokenizer itr1 = new StringTokenizer(str1);
					itr1.nextToken();
					String str2 = itr1.nextToken();
					StringTokenizer itr2 = new StringTokenizer(str2, ":");
					String str = itr2.nextToken();*/
				if (str.length() == 0) continue;

					word.set(timeFormat(str));
				context.write(word, one);
				//for(int i=0; i < 17 ; i++) {itr.nextToken();}
			}
		}

		private static String timeFormat(String input) {
			int value = Integer.parseInt(input);
			if (value > 12)
				return String.valueOf(value - 12) + "pm";
			else
				return String.valueOf(value) + "am";
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
	/* your code goes in here */

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: MapRedSolution2 <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "MapRed Solution #2");
		
		
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

		String[] validMd5Sums = { "03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83",
				"07f6514a2f48cff8e12fdbc533bc0fe5", "e3c247d186e3f7d7ba5bab626a8474d7",
				"fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd",
				"6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466",
				"7d35ce45afd621e46840627a79f87dac" };

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
