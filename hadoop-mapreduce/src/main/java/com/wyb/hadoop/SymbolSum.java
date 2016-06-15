package com.wyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SymbolSum {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		// Create configuration
		Configuration conf = new Configuration(true);
		//conf.set("fs.default.name", "hdfs://" + host + ":9000");
        //conf.set("mapred.job.tracker", "hdfs://" + host + ":8021");

		// Create job
		Job job = new Job(conf, "SymbolSum");
		job.setJarByClass(SymbolSumMapper.class);

		// Setup MapReduce
		job.setMapperClass(SymbolSumMapper.class);
		job.setReducerClass(SymbolSumReducer.class);
		job.setNumReduceTasks(0);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Input
		KeyValueTextInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

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