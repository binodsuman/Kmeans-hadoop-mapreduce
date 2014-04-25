package com.zikesjan.bigdata.kmenas;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zikesjan.bigdata.KmeansMain.MyCounters;


public class KmeansReducer extends Reducer<IntWritable, PointWritable, Text, Text>{

	private LinkedHashMap<String, MutableDouble> newMeanMap = new LinkedHashMap<String, MutableDouble>();
	private PointWritable newMeanObject;
	private List<PointWritable> means;
	
	public void setup(Context context) throws IOException, InterruptedException {
		means = new ArrayList<PointWritable>();
		Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br = new BufferedReader(new FileReader(localCacheFiles[0].toString()));
		String lineString = br.readLine();
		while(lineString != null){
			String[] keyValue = lineString.split("\t");
			PointWritable mwc = new PointWritable();
			mwc.addAllFeaturesFromString(keyValue[1]);
			means.add(mwc);
			lineString = br.readLine();
		}
		br.close();
	}
	
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
	
		
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		for(PointWritable pwc : means){
			if(newMeanObject.equals(pwc)){
				context.getCounter(MyCounters.Changed).increment(1);
			}
		}
	}
	
}
