package com.wyb.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MockupTaskMapper extends Mapper<LongWritable,Text, Text, DoubleWritable> {

	private  DoubleWritable closePrice = new DoubleWritable(1);
	private Text month = new Text();
	private List<String> data;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Thread.sleep(Long.parseLong(value.toString()));
		month.set("yeah");
		context.write(month, closePrice);
	}

	@Override
	protected void setup(Mapper<LongWritable,Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = new Configuration(true);
		FileSystem hdfs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs:///user/hive/warehouse/eurusd/EURUSD.csv"))));
		data = new ArrayList<String>();
		String line = null;
		while((line = br.readLine())!=null){
			data.add(line);
		}
		br.close();
		
	}
	
	
}
