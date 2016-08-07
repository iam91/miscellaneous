package com.zwy.hadoopml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zwy.hadoopml.core.HadoopFile;
import com.zwy.hadoopml.logisticregression.LogisticRegressionMapper;
import com.zwy.hadoopml.logisticregression.LogisticRegressionReducer;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		String file = "/home/zwy/workspace/miscellaneous/resources/data_banknote_authentication.csv";
		String modelFile = "/home/zwy/workspace/miscellaneous/resources/iter/model";
		
		double alpha = 0.05;
		int numOfIteration = 100;
		int numOfFeature = 5;
		int numOfInstances = 1372;
		int targetValIndex = numOfFeature - 1;
		
		for(int i = 0; i < numOfIteration; i++){
			
			Configuration conf = new Configuration();
			
			HadoopFile model = new HadoopFile(modelFile + i +"/part-r-00000", conf);
			BufferedReader reader = new BufferedReader(new InputStreamReader(model.getInputStream()));
			
			conf.set("mapreduce.output.textoutputformat.separator", ":");
			
			conf.setDouble("ALPHA", alpha);
			conf.setInt("N_ITER", numOfIteration);
			conf.setInt("N_FEATURE", numOfFeature);
			conf.setInt("N_INSTANCE", numOfInstances);
			conf.setInt("TARGET_ID", targetValIndex);
			conf.set("THETA", reader.readLine().split(":")[1]);
			
			reader.close();
			model.closeFile();
			//model.deleteFile();
		
			Job job = Job.getInstance(conf, "Test");
			job.setJarByClass(com.zwy.hadoopml.Driver.class);
			
			job.setMapperClass(LogisticRegressionMapper.class);
			job.setReducerClass(LogisticRegressionReducer.class);
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path(file));
			FileOutputFormat.setOutputPath(job, new Path(modelFile + (i + 1)));
			
			if (!job.waitForCompletion(true)){
				return;
			}
		}
	}
}
