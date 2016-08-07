package com.zwy.hadoopml.core;

public class Instance {
	private int numOfFeatures;
	
	private double[] features;
	
	public Instance(int numOfFeatures){
		this.numOfFeatures = numOfFeatures;
		features = new double[numOfFeatures];
	}
	
	public void fillFeature(int index, double value){
		if(index < numOfFeatures){
			features[index] = value;
		}
	}
	
	public void fillFeature(double[] features){
		if(features.length == numOfFeatures){
			for(int i = 0; i < numOfFeatures; i++){
				this.features[i] = features[i];
			}
		}
	}
	
	public double getFeature(int index){
		return this.features[index];
	}
	
	public double[] getFeature(){
		return this.features;
	}
}
