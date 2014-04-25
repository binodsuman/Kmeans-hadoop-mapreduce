package com.zikesjan.bigdata.kmenas;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable{

	private LinkedHashMap<String, MutableDouble> features;
	
	public PointWritable(){
		this.features = new LinkedHashMap<String, MutableDouble>();
	}
	

	public PointWritable(LinkedHashMap<String, MutableDouble> features) {
		super();
		this.features = features;
	}
	
	public LinkedHashMap<String, MutableDouble> getFeatures(){
		return this.features;
	}
	
	public void addFeature(String word, double tfidf){
		features.put(word, new MutableDouble(tfidf));
	}
	
	public void addFeatureFromString(String feature){
		String[] splited = feature.split(":");
		features.put(splited[0], new MutableDouble(Double.parseDouble(splited[1])));
	}
	
	public void addAllFeaturesFromString(String data){
		String[] features = data.split(" ");
		for(String feature : features){
			addFeatureFromString(feature);
		}
	}
	
	/**
	 * calculation of the eucleudian distance. (works only for the small data, otherwise it's too slow)
	 * @param other
	 * @return
	 */
	public double eucleidianDistanceToOther(PointWritable other){
		double result = 0.0;
		for(String s : features.keySet()){
			if(other.features.containsKey(s)){
				result += (features.get(s).doubleValue() - other.features.get(s).doubleValue()) * (features.get(s).doubleValue() - other.features.get(s).doubleValue());
			}else{
				result += features.get(s).doubleValue() * features.get(s).doubleValue();
			}
		}
		for(String s : other.features.keySet()){
			if(!features.containsKey(s)){
				result += other.features.get(s).doubleValue() * other.features.get(s).doubleValue();
			}
		}
		return Math.sqrt(result); 	
	}
	
	
	/**
	 * calculation of the cosine norm for the spherical clustering, vectors must be normalized before the call of this method
	 * @param other
	 * @return
	 */
	public double cosineNorm(PointWritable other){
		double result = 0.0;
		if(features.keySet().size() < other.features.keySet().size()){
			for(String s : features.keySet()){
				if(other.features.containsKey(s)){
					result += features.get(s).doubleValue() * other.features.get(s).doubleValue();
				}
			}
		}else{
			for(String s : other.features.keySet()){
				if(features.containsKey(s)){
					result += features.get(s).doubleValue() * other.features.get(s).doubleValue();
				}
			}
		}
		return result;
	}
	
	/**
	 * vector normalization
	 */
	public void normalize(){
		double divisor = 0.0;
		for(String s : features.keySet()){
			divisor += features.get(s).doubleValue() * features.get(s).doubleValue();
		}
		divisor = Math.sqrt(divisor);
		for(String s : features.keySet()){
			//TODO think of more elegant solution here
			double tmp = features.get(s).doubleValue();
			features.put(s, new MutableDouble(tmp/divisor));
		}
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Text t = new Text();
		t.readFields(in);
		features = new LinkedHashMap<String, MutableDouble>();
		addAllFeaturesFromString(t.toString());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringBuilder sb = new StringBuilder();
		for(String s : features.keySet()){
			sb.append(s+":"+features.get(s)+" ");
		}
		new Text(sb.toString()).write(out);
	}


	public String getFeaturesString(){
		StringBuilder sb = new StringBuilder();
		for(String s : features.keySet()){
			sb.append(s+":"+features.get(s)+" ");
		}
		return sb.toString();
	}

	
	/**
	 * careful this thing is probably not working so don't use hash sets nor hash map of these objects
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((features == null) ? 0 : features.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PointWritable other = (PointWritable) obj;
		boolean same = true;
		for(String key : features.keySet()){
			if(!other.features.containsKey(key)){
				same = false;
				break;
			}else{
				if(Math.abs(other.features.get(key).doubleValue() - this.features.get(key).doubleValue()) > 0.0001){	//because of the floating point precission
					same = false;
					break;
				}
			}
		}
		if(!same) return false;
		for(String key : other.features.keySet()){
			if(!features.containsKey(key)){
				same = false;
				break;
			}else{
				if(Math.abs(other.features.get(key).doubleValue() - this.features.get(key).doubleValue()) > 0.0001){	//because of the floating point precission
					same = false;
					break;
				}
			}
		}
		return same;
	}
}
