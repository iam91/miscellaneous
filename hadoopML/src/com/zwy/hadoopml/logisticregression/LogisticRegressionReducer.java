package com.zwy.hadoopml.logisticregression;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class LogisticRegressionReducer extends Reducer<LongWritable, ArrayPrimitiveWritable, Text, Text>{
	private double[] theta;
	private double[] sum;
	private Configuration conf;
	
	protected void setup(Context context){
		conf = context.getConfiguration();
		theta = ArrayStringTrans.stringToDoubleArray(conf.get("THETA"));
		sum = new double[theta.length];
	}
	
	public void reduce(LongWritable key, Iterable<ArrayPrimitiveWritable> val, Context context) throws IOException, InterruptedException{
		for(ArrayPrimitiveWritable v: val){
			double[] t = (double[])v.get();
			for(int i = 0; i < theta.length; i++){
				sum[i] += t[i];
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		for(int i = 0; i < theta.length; i++){
			theta[i] += sum[i];
		}
		context.write(new Text("model"), new Text(ArrayStringTrans.doubleArrayToString(theta)));
	}
}
