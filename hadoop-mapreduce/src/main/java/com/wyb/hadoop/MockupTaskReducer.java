package com.wyb.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MockupTaskReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		for (DoubleWritable value : values) {
			context.write(text,value);
		}
	}
}
