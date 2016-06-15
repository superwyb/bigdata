package com.wyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SymbolSumMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	private  DoubleWritable closePrice = new DoubleWritable(1);
	private Text month = new Text();
	private double count = 0;

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] csv = key.toString().split(",");
		if(csv.length != 7)return;
		if(csv[0].length()!=10)return;
		month.set(csv[0].substring(0,7));
		closePrice.set(count);
		context.write(month, closePrice);
		count+=1.0;;
	}

	@Override
	protected void setup(Mapper<Text, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}
	
	
}
