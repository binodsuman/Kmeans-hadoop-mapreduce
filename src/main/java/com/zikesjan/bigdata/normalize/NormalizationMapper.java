package com.zikesjan.bigdata.normalize;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zikesjan.bigdata.KmeansMain.MyCounters;

public class NormalizationMapper extends Mapper<Text, Text, Text, Text>{

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.getCounter(MyCounters.Documents).increment(1);
		context.write(key, value);
	}
	
}
