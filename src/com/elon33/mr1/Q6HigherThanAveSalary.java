package com.elon33.mr1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q6HigherThanAveSalary extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split(",");

			context.write(new IntWritable(0), new Text(kv[5]));

			context.write(new IntWritable(1), new Text(kv[1] + "," + kv[5]));
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

		private long allSalary = 0;
		private int allEmpCount = 0;
		private long aveSalary = 0;
		
		private long empSalary = 0;

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				if (0 == key.get()) {
					allSalary += Long.parseLong(val.toString());
					allEmpCount++;
					System.out.println("allEmpCount = " + allEmpCount);
				} else if (1 == key.get()) {
					if (aveSalary == 0) {
						aveSalary = allSalary / allEmpCount;
						context.write(new Text("Average Salary = "), new Text("" + aveSalary));
						context.write(new Text("Following employees have salarys higher than Average:"), new Text(""));
					}

					System.out.println("Employee salary = " + val.toString());
					aveSalary = allSalary / allEmpCount;
					
					empSalary = Long.parseLong(val.toString().split(",")[1]);
					if (empSalary > aveSalary) {
						context.write(new Text(val.toString().split(",")[0]), new Text("" + empSalary));
					}
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Q6HigherThanAveSalary");
		job.setJobName("Q6HigherThanAveSalary");

		job.setJarByClass(Q6HigherThanAveSalary.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q6HigherThanAveSalary(), args);
		System.exit(res);
	}
}
