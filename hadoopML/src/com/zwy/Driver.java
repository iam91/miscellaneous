package com.zwy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zwy.ml.logisticregression.ArrayStringTrans;
import com.zwy.ml.logisticregression.LogisticRegressionMapper;
import com.zwy.ml.logisticregression.LogisticRegressionReducer;

import java.io.IOException;

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String file = "/home/zwy/workspace/miscellaneous/resources/data_banknote_authentication.csv";
		
		double alpha = 0.05;
		int numOfIteration = 100;
		int numOfFeature = 5;
		int numOfInstances = 1372;
		int targetValIndex = numOfFeature - 1;
		double[] theta = new double[numOfFeature];
		for(int i = 0; i < theta.length; i++){
			theta[i] = 0.0;
		}
		
		Configuration conf = new Configuration();
		
		conf.setDouble("ALPHA", alpha);
		conf.setInt("N_ITER", numOfIteration);
		conf.setInt("N_FEATURE", numOfFeature);
		conf.setInt("N_INSTANCE", numOfInstances);
		conf.setInt("TARGET_ID", targetValIndex);
		conf.set("THETA", ArrayStringTrans.doubleArrayToString(theta));
		
		System.out.println(conf.get("THETA"));
		
		//for()
		
		Job job = Job.getInstance(conf, "Test");
		job.setJarByClass(com.zwy.Driver.class);
		
		job.setMapperClass(LogisticRegressionMapper.class);
		job.setReducerClass(LogisticRegressionReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayPrimitiveWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(file));
		FileOutputFormat.setOutputPath(job, new Path(file + "-out"));
		
		if (!job.waitForCompletion(true)){
		}
	}
}
