package com.wyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SymbolSumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
			count++;
		}
		context.write(text, count == 0? new DoubleWritable(0): new DoubleWritable(sum/count));
	}
}
