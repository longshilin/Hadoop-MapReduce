package com.elon33.mr1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

public class Q1SumDeptSalary extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		// 用于缓存 dept文件中的数据
		private Map<String, String> deptMap = new HashMap<String, String>();
		private String[] kv;

		// 此方法会在Map方法执行之前执行且执行一次，【缓存阶段】
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader in = null;
			try {

				// 从当前作业中获取要缓存的文件
				URI[] paths = DistributedCache.getCacheFiles(context.getConfiguration());
				String deptIdName = null;
				for (URI path : paths) {

					// 对部门文件字段进行拆分并缓存到deptMap中
					if (path.toString().contains("dept")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while (null != (deptIdName = in.readLine())) {

							// 对部门文件字段进行拆分并缓存到deptMap中
							// 其中Map中key为部门编号，value为所在部门名称
							deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// 【map阶段】 输入的文件自动作为map的输入参数，一行为一个key
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 对员工文件字段进行拆分
			kv = value.toString().split(",");

			// map join: 在map阶段过滤掉不需要的数据(保证只有deptMap中存在的部门号)，
			// 在此阶段完成了emp中的部门号和dept中的部门名之间的关联，输出key为部门名称和value为员工工资
			if (deptMap.containsKey(kv[7])) {
				if (null != kv[5] && !"".equals(kv[5].toString())) {
					context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
					// System.out.println("<"+deptMap.get(kv[7].trim())+"--"+
					// kv[5].trim()+">");
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 将多个相同key的小键值对 在shuffle阶段已经汇聚为一个整体键值对
			// 对同一部门的员工工资进行求和
			long sumSalary = 0;
			for (Text val : values) {
				sumSalary += Long.parseLong(val.toString());
			}

			// 输出key为部门名称和value为该部门员工工资总和
			context.write(key, new LongWritable(sumSalary));
			// System.out.println("<"+key+" --"+sumSalary+">");
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		// 实例化作业对象，设置作业名称、Mapper和Reduce类
		Job job = new Job(getConf(), "Q1SumDeptSalary");
		job.setJobName("Q1SumDeptSalary");
		job.setJarByClass(Q1SumDeptSalary.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);

		// 设置输出格式
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
		String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
		DistributedCache.addCacheFile(new URI(otherArgs[0]), job.getConfiguration());
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	/**
	 * 主方法，执行入口
	 * 
	 * @param args
	 *            输入参数
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Q1SumDeptSalary(), args);
		System.exit(res);
	}
}