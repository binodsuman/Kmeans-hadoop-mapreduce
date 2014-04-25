package com.zikesjan.bigdata.marker;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zikesjan.bigdata.kmenas.PointWritable;

public class MarkerMapper extends Mapper<Text, Text, Text, Text>{

	public List<PointWritable> means;
	
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
		context.write(new Text(findClosest(pwc)+":"+key.toString()), value);	
	}
	
	
	/**
	 * method that returns the closest mean from the point
	 * @param value
	 * @return
	 */
	private int findClosest(PointWritable value){
		int closest = -1;
		double minimalDistance = Double.MAX_VALUE;
		for(int i = 0; i<means.size(); i++){
			double distance = value.cosineNorm(means.get(i));
			if(distance < minimalDistance){
				minimalDistance = distance;
				closest = i;
			}
		}
		return closest;
	}
}
