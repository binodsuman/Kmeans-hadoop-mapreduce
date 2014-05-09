package com.zikesjan.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.zikesjan.bigdata.kmenas.KmeansMapper;
import com.zikesjan.bigdata.kmenas.KmeansReducer;
import com.zikesjan.bigdata.kmenas.PointWritable;
import com.zikesjan.bigdata.marker.MarkerMapper;
import com.zikesjan.bigdata.marker.MarkerReducer;
import com.zikesjan.bigdata.normalize.NormalizationMapper;
import com.zikesjan.bigdata.normalize.NormalizationReducer;
import com.zikesjan.bigdata.sample.SampleMapper;
import com.zikesjan.bigdata.sample.SampleReducer;

public class KmeansMain {
	
	//addresses of the helping storage directories
    private static final String OUTPUT_PATH = "/user/biadmin/output/means";
    private static final String OUTPUT_PATH_NORM = "/user/biadmin/output/normalized";
    private static final String CACHE_PATH = "/user/biadmin/output/cache";
    //addresses of the means files
    private static final String CACHED_MEANS = "/user/biadmin/output/cache/part-r-00000";
    private static final String ACTUAL_MEANS = "/user/biadmin/output/means/part-r-00000";
    

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException, URISyntaxException {

		Path inputPath = new Path(args[3]);
		Path outputDir = new Path(args[4]);
		int k = Integer.parseInt(args[0]);
		int maxIterations = Integer.parseInt(args[1]);
		double threshold = Double.parseDouble(args[2]);
		
		// Create configuration
		Configuration conf = new Configuration(true);
		Job normalize = new Job(conf, "Normalize");
		normalize.setJarByClass(NormalizationMapper.class);
		normalize.setMapperClass(NormalizationMapper.class);
		normalize.setMapOutputKeyClass(Text.class);
		normalize.setMapOutputValueClass(Text.class);
		normalize.setReducerClass(NormalizationReducer.class);
		normalize.setOutputKeyClass(Text.class);
		normalize.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(normalize, inputPath);
		normalize.setInputFormatClass(KeyValueTextInputFormat.class);
		Path normPath = new Path(OUTPUT_PATH_NORM);
		FileOutputFormat.setOutputPath(normalize, normPath);
		normalize.setOutputFormatClass(TextOutputFormat.class);
		
		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(normPath))
			hdfs.delete(normPath, true);

		// Execute job
		int code = normalize.waitForCompletion(true) ? 0 : 1;
		int documents = (int) normalize.getCounters().findCounter(MyCounters.Documents).getValue();
		
		conf.set("k", k+"");							//passing K to the map reduce as a parameter
		conf.set("documents", documents+"");
		
		//Create job that will select random sample
		Job sample = new Job(conf, "Smaple");
		sample.setJarByClass(SampleMapper.class);
		sample.setMapperClass(SampleMapper.class);
		sample.setMapOutputKeyClass(Text.class);
		sample.setMapOutputValueClass(Text.class);
		sample.setReducerClass(SampleReducer.class);
		sample.setNumReduceTasks(1);					//it is very important to have only one reducer for sampling here
		sample.setOutputKeyClass(Text.class);
		sample.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(sample, normPath);
		sample.setInputFormatClass(KeyValueTextInputFormat.class);
		Path meansPath = new Path(OUTPUT_PATH);
		FileOutputFormat.setOutputPath(sample, meansPath);
		sample.setOutputFormatClass(TextOutputFormat.class);
		
		// Delete output if exists
		if (hdfs.exists(meansPath))
			hdfs.delete(meansPath, true);

		// Execute job
		code = sample.waitForCompletion(true) ? 0 : 1;
	
		
		Path cache = new Path(CACHE_PATH);
		boolean changed = false;
		int counter = 0;
		while(!changed && counter < maxIterations){  
			
			conf.set("threshold", threshold+"");
			Job kmeans = new Job(conf, "Kmeans");
			
			if(hdfs.exists(cache))
				hdfs.delete(cache, true);
			hdfs.rename(meansPath, cache);	 //moving the previous iteration file to the cache directory
			DistributedCache.addCacheFile(new URI(CACHED_MEANS), kmeans.getConfiguration());
			
			kmeans.setJarByClass(KmeansMapper.class);
			kmeans.setMapperClass(KmeansMapper.class);
			kmeans.setMapOutputKeyClass(IntWritable.class);
			kmeans.setMapOutputValueClass(PointWritable.class);
			kmeans.setReducerClass(KmeansReducer.class);
			kmeans.setOutputKeyClass(Text.class);
			kmeans.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(kmeans, normPath);
			kmeans.setInputFormatClass(KeyValueTextInputFormat.class);
			FileOutputFormat.setOutputPath(kmeans, meansPath);
			kmeans.setOutputFormatClass(TextOutputFormat.class);
			
			// Delete output if exists
			if (hdfs.exists(meansPath))
				hdfs.delete(meansPath, true);

			// Execute job
			code = kmeans.waitForCompletion(true) ? 0 : 1;
			
			//checking if the mean is stable
			BufferedReader file1Reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(CACHED_MEANS))));
			BufferedReader file2Reader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(ACTUAL_MEANS))));
			for(int i = 0; i<k; i++){
				String file1String = file1Reader.readLine();
				String file2String = file2Reader.readLine();
				String[] keyValue1 = file1String.split("\t");
				PointWritable mwc1 = new PointWritable();
				mwc1.addAllFeaturesFromString(keyValue1[1]);
				String[] keyValue2 = file2String.split("\t");
				PointWritable mwc2 = new PointWritable();
				mwc2.addAllFeaturesFromString(keyValue2[1]);
				mwc1.setThreshold(threshold);
				if(mwc1.equals(mwc2)){
					changed = true;
				}else{
					changed = false;
					break;
				}
			}
			file1Reader.close();
			file2Reader.close();
			System.out.println("KMEANS finished iteration:>> "+counter + " || means stable: "+ changed);
			counter++;
		}
		
		Job marking = new Job(conf, "Marking");
		DistributedCache.addCacheFile(new URI("/user/biadmin/output/means/part-r-00000"), marking.getConfiguration());
		marking.setJarByClass(MarkerMapper.class);
		marking.setMapperClass(MarkerMapper.class);
		marking.setMapOutputKeyClass(Text.class);
		marking.setMapOutputValueClass(Text.class);
		marking.setReducerClass(MarkerReducer.class);
		marking.setOutputKeyClass(Text.class);
		marking.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(marking, inputPath);
		marking.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(marking, outputDir);
		marking.setOutputFormatClass(TextOutputFormat.class);
		
		// Delete output if exists
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		code = marking.waitForCompletion(true) ? 0 : 1;
		
		
		System.exit(code);
	}
	
	public enum MyCounters {
		Actual,
		Documents,
	}
}
