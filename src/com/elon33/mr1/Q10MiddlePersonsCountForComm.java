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

		// 在Mapper阶段所有员工数据，其中经理数据key为0值、value为"员工编号，员工经理编号"
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] kv = value.toString().split(",");

			context.write(new IntWritable(0), new Text(kv[0] + "," + ("".equals(kv[3]) ? " " : kv[3])));
		}
	}

	// 在Reduce阶段把所有员工放到员工列表和员工对应经理链表Map中
	public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

		List<String> employeeList = new ArrayList<String>(); // 存放员工编号的链表
		Map<String, String> employeeToManagerMap = new HashMap<String, String>(); // 存放员工编号与其上司经理编号的映射关系

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				employeeList.add(value.toString().split(",")[0].trim());
				employeeToManagerMap.put(value.toString().split(",")[0].trim(), value.toString().split(",")[1].trim());
			}
		}

		@Override
		// 最后在Reduce的Cleanup中按照上面所说算法对任意两个员工计算出沟通的路径长度并输出。
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int totalEmployee = employeeList.size();
			int i, j;
			int distance;
			System.out.println(employeeList);
			System.out.println(employeeToManagerMap);
			// 输出任意两个员工之间的连通路径长度
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

			if (employeeToManagerMap.get(employeeA).equals(employeeB)
					|| employeeToManagerMap.get(employeeB).equals(employeeA)) {
				distance = 0;
			} else if (employeeToManagerMap.get(employeeA).equals(employeeToManagerMap.get(employeeB))) {
				distance = 0;
			} else {
				List<String> employeeA_ManagerList = new ArrayList<String>();
				List<String> employeeB_ManagerList = new ArrayList<String>();

				employeeA_ManagerList.add(employeeA); // A链表中插入A员工编号
				String current = employeeA;
				while (false == employeeToManagerMap.get(current).isEmpty()) {
					current = employeeToManagerMap.get(current);
					employeeA_ManagerList.add(current); // 通过A员工编号得到对应的上司经理的编号，并插入链表,最终得到从A开始，且存放向上追溯相关联的经理节点的链表
				}

				employeeB_ManagerList.add(employeeB); // B链表中插入B员工编号
				current = employeeB;
				while (false == employeeToManagerMap.get(current).isEmpty()) {
					current = employeeToManagerMap.get(current);
					employeeB_ManagerList.add(current); // 通过B员工编号得到对应的上司经理的编号，并插入链表,最终得到从B开始，且存放向上追溯相关联的经理节点的链表
				}

				int ii = 0, jj = 0;
				String currentA_manager, currentB_manager;
				boolean found = false;

				// 找到A和B链表交集，得到路径长度
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
