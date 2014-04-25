package com.zikesjan.bigdata.sample;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zikesjan.bigdata.KmeansMain.MyCounters;

public class SampleReducer extends Reducer<Text, Text, Text, Text>{

	private HashSet<Integer> selected;
		
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("k"));
		int documents = Integer.parseInt(conf.get("documents"));
		setRandoms(documents, k);
	}

	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException{
		if(selected.contains((int) context.getCounter(MyCounters.Actual).getValue())){
			context.write(key, values.iterator().next());		
		}      
		context.getCounter(MyCounters.Actual).increment(1);
	}
	
	public void setRandoms(int totalRows, int k){
		Random rnd = new Random();
		selected = new HashSet<Integer>();
		for(int i = 0; i<k; i++){
			int rowNumber = rnd.nextInt(totalRows);
			while(selected.contains(rowNumber)){
				rowNumber = rnd.nextInt(totalRows);
			}
			selected.add(rowNumber);
		}
	}
	
	
}
