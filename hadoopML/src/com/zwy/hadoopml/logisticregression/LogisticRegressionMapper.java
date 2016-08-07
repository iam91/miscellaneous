package com.zwy.hadoopml.logisticregression;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LogisticRegressionMapper extends Mapper<LongWritable, Text, LongWritable, ArrayPrimitiveWritable>{
	private int numOfFeature;
	private int numOfInstance;
	private int targetValIndex;
	private double alpha;
	private double[] theta;
	
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		numOfFeature = conf.getInt("N_FEATURE", 0);
		numOfInstance = conf.getInt("N_INSTANCE", 0);
		targetValIndex = conf.getInt("TARGET_ID", numOfFeature - 1);
		alpha = conf.getDouble("ALPHA", 1.0);
		theta = ArrayStringTrans.stringToDoubleArray(conf.get("THETA"));
	}
	
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException{
		String[] split = val.toString().split(",");
		double[] x = new double[numOfFeature];
		for(int i = 0; i < numOfFeature - 1; i++){
			x[i] = Double.parseDouble(split[i]);
		}
		x[numOfFeature - 1] = 1.0;
		double y = Double.parseDouble(split[targetValIndex]);
		
		double[] t = new double[numOfFeature];
		for(int j = 0; j < numOfFeature; j++){
			t[j] = (y - logistic(dotProduct(theta, x))) * alpha * x[j] / numOfInstance;
		}
		
		context.write(key, new ArrayPrimitiveWritable(t));
	}
	
	private double dotProduct(double[] theta, double[] x){
		double product = 0.0;
		for(int i = 0; i < theta.length; i++){
			product += theta[i] * x[i];
		}
		return product;
	}
	
	private double logistic(double u){
		return 1 / (1 + Math.pow(Math.E, -u));
	}
}