package com.elon33.mr1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q5EarnMoreThanManager extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split(",");

			context.write(new Text(kv[0].toString()), new Text("M," + kv[5]));

			if (null != kv[3] && !"".equals(kv[3].toString())) {
				context.write(new Text(kv[3].toString()), new Text("E," + kv[1] + "," + kv[5]));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String empName;
			long empSalary = 0;
			HashMap<String, Long> empMap = new HashMap<String, Long>();
			
			long mgrSalary = 0;

			for (Text val : values) {
				if (val.toString().startsWith("E")) {
					empName = val.toString().split(",")[1];
					empSalary = Long.parseLong(val.toString().split(",")[2]);
					empMap.put(empName, empSalary);
				} else {
					mgrSalary = Long.parseLong(val.toString().split(",")[1]);
				}
			}

			for (java.util.Map.Entry<String, Long> entry : empMap.entrySet()) {
				if (entry.getValue() > mgrSalary) {
					context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Q5EarnMoreThanManager");
		job.setJobName("Q5EarnMoreThanManager");

		job.setJarByClass(Q5EarnMoreThanManager.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q5EarnMoreThanManager(), args);
		System.exit(res);
	}
}
