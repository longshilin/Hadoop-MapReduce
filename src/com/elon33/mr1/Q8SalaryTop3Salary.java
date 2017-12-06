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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q8SalaryTop3Salary extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split(",");
			// <员工姓名,工资>
			context.write(new IntWritable(0), new Text(kv[1].trim() + "," + kv[5].trim()));
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String empName;
			String firstEmpName = "";
			String secondEmpName = "";
			String thirdEmpName = "";
			
			long empSalary = 0;
			long firstEmpSalary = 0;
			long secondEmpSalary = 0;
			long thirdEmpSalary = 0;

			for (Text val : values) {
				empName = val.toString().split(",")[0];
				empSalary = Long.parseLong(val.toString().split(",")[1]);
				
				if(empSalary > firstEmpSalary) {
					thirdEmpName = secondEmpName;
					thirdEmpSalary = secondEmpSalary;
					secondEmpName = firstEmpName;
					secondEmpSalary = firstEmpSalary;
					firstEmpName = empName;
					firstEmpSalary = empSalary;
				} else if (empSalary > secondEmpSalary) {
					thirdEmpName = secondEmpName;
					thirdEmpSalary = secondEmpSalary;
					secondEmpName = empName;
					secondEmpSalary = empSalary;
				} else if (empSalary > thirdEmpSalary) {
					thirdEmpName = empName;
					thirdEmpSalary = empSalary;
				}
			}
			
			context.write(new Text( "First employee name:" + firstEmpName), new Text("Salary:" + firstEmpSalary));
			context.write(new Text( "Second employee name:" + secondEmpName), new Text("Salary:" + secondEmpSalary));
			context.write(new Text( "Third employee name:" + thirdEmpName), new Text("Salary:" + thirdEmpSalary));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Q8SalaryTop3Salary");
		job.setJobName("Q8SalaryTop3Salary");

		job.setJarByClass(Q8SalaryTop3Salary.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputValueClass(Text.class);

		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q8SalaryTop3Salary(), args);
		System.exit(res);
	}
}
