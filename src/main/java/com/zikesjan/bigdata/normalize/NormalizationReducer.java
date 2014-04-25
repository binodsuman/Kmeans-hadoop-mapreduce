package com.zikesjan.bigdata.normalize;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NormalizationReducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException{
		String[] features = values.iterator().next().toString().split(" ");
		double norm = 0.0;
		double[] vals = new double[features.length];
		for(int i = 0; i<features.length; i++){
			String[] ft = features[i].split(":");
			vals[i] = Double.parseDouble(ft[1]); 
			norm += (vals[i]*vals[i]);
		}
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i< features.length; i++){
			sb.append(features[i].split(":")[0]).append(":").append(vals[i]/Math.sqrt(norm)).append(" ");
		}
		sb.deleteCharAt(sb.length()-1);
		context.write(key, new Text(sb.toString()));
	}
	
}
