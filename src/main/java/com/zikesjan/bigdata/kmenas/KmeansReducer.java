package com.zikesjan.bigdata.kmenas;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class KmeansReducer extends Reducer<IntWritable, PointWritable, Text, Text>{

	private LinkedHashMap<String, MutableDouble> newMeanMap = new LinkedHashMap<String, MutableDouble>();
	private PointWritable newMeanObject;
	double threshold;
	
		
	public void reduce(IntWritable key, Iterable<PointWritable> values, Context context) throws IOException, 
    InterruptedException{
		
		for(PointWritable mwc : values){
			for(String featureKey : mwc.getFeatures().keySet()){
				if(newMeanMap.containsKey(featureKey)){
					newMeanMap.get(featureKey).add(mwc.getFeatures().get(featureKey));				
				}else{
					newMeanMap.put(featureKey, new MutableDouble(mwc.getFeatures().get(featureKey)));
				}
			}
		}
		
		newMeanObject = new PointWritable(newMeanMap);
		newMeanObject.normalize();
		context.write(new Text("xxx"), new Text(newMeanObject.getFeaturesString()));
		
	}	
}
