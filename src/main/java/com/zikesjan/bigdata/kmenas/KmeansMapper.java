package com.zikesjan.bigdata.kmenas;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMapper extends Mapper<Text, Text, IntWritable, PointWritable>{
	
	private List<PointWritable> means;
	
	/**
	 * reading the data from the distributed cache
	 */
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
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		PointWritable pwc = new PointWritable();
		pwc.addAllFeaturesFromString(value.toString());
		context.write(new IntWritable(findClosest(pwc)), pwc);	
	}
	
	/**
	 * method that returns the closest mean from the point
	 * @param value
	 * @return
	 */
	private int findClosest(PointWritable value){
		int argmax = 0;
		double minimalDistance = 0.0;
		for(int i = 0; i<means.size(); i++){
			PointWritable pwc = means.get(i);
			double distance = value.cosineNorm(pwc);
			if(distance > minimalDistance){
				minimalDistance = distance;
				argmax = i;
			}
		}
		return argmax;
	}
}
