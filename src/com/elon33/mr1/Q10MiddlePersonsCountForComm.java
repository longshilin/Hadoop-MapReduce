package com.elon33.mr1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Q10MiddlePersonsCountForComm extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split(",");

			context.write(new IntWritable(0), new Text(kv[0] + "," + ("".equals(kv[3]) ? " " : kv[3])));
		}
	}

	public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

		List<String> employeeList = new ArrayList<String>();
		Map<String, String> employeeToManagerMap = new HashMap<String, String>();

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				employeeList.add(value.toString().split(",")[0].trim());
				employeeToManagerMap.put(value.toString().split(",")[0].trim(), value.toString().split(",")[1].trim());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int totalEmployee = employeeList.size();
			int i, j;
			int distance;
			System.out.println(employeeList);
			System.out.println(employeeToManagerMap);

			for (i = 0; i < (totalEmployee - 1); i++) {
				for (j = (i + 1); j < totalEmployee; j++) {
					distance = calculateDistance(i, j);
					String value = employeeList.get(i) + " and " + employeeList.get(j) + " = " + distance;
					context.write(NullWritable.get(), new Text(value)); 
				}
			}
		}

		private int calculateDistance(int i, int j) {
			String employeeA = employeeList.get(i);
			String employeeB = employeeList.get(j);
			int distance = 0;

			if (employeeToManagerMap.get(employeeA).equals(employeeB) || employeeToManagerMap.get(employeeB).equals(employeeA)) {
				distance = 0;
			}
			else if (employeeToManagerMap.get(employeeA).equals(employeeToManagerMap.get(employeeB))) {
				distance = 0;
			} else {
				List<String> employeeA_ManagerList = new ArrayList<String>();
				List<String> employeeB_ManagerList = new ArrayList<String>();

				employeeA_ManagerList.add(employeeA);
				String current = employeeA;
				while (false == employeeToManagerMap.get(current).isEmpty()) {
					current = employeeToManagerMap.get(current);
					employeeA_ManagerList.add(current);
				}

				employeeB_ManagerList.add(employeeB);
				current = employeeB;
				while (false == employeeToManagerMap.get(current).isEmpty()) {
					current = employeeToManagerMap.get(current);
					employeeB_ManagerList.add(current);
				}

				int ii = 0, jj = 0;
				String currentA_manager, currentB_manager;
				boolean found = false;

				for (ii = 0; ii < employeeA_ManagerList.size(); ii++) {
					currentA_manager = employeeA_ManagerList.get(ii);
					for (jj = 0; jj < employeeB_ManagerList.size(); jj++) {
						currentB_manager = employeeB_ManagerList.get(jj);
						if (currentA_manager.equals(currentB_manager)) {
							found = true;
							break;
						}
					}

					if (found) {
						break;
					}
				}

				distance = ii + jj - 1;
			}

			return distance;
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "Q10MiddlePersonsCountForComm");
		job.setJobName("Q10MiddlePersonsCountForComm");

		job.setJarByClass(Q10MiddlePersonsCountForComm.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q10MiddlePersonsCountForComm(), args);
		System.exit(res);
	}
}
