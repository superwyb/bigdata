package com.wyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MockupTask {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//String host = "myhadoop1-3967.lvs01.dev.ebayc3.com";
		//Path inputPath = new Path("/user/yingbwang/input");
		//Path outputDir = new Path("/user/yingbwang/output");
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		int line = Integer.parseInt(args[2]);
		// Create configuration
		Configuration conf = new Configuration(true);
		//conf.set("fs.default.name", "hdfs://" + host + ":9000");
        //conf.set("mapred.job.tracker", "hdfs://" + host + ":8021");

		// Create job
		Job job = new Job(conf, "MockupTask");
		job.setJarByClass(MockupTaskMapper.class);

		// Setup MapReduce
		job.setMapperClass(MockupTaskMapper.class);
		job.setReducerClass(MockupTaskReducer.class);
		job.setNumReduceTasks(2);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input
		NLineInputFormat.addInputPath(job, inputPath);
		NLineInputFormat.setNumLinesPerSplit(job, line);
		job.setInputFormatClass(NLineInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

}