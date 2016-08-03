package com.zwy.core;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class FileReader {
	private String filePath;
	private Configuration conf;
	private DataSet dataSet;
	
	public FileReader(String filePath, Configuration conf) throws IOException{
		this.filePath = filePath;
		this.conf = conf;
		
		Path path = new Path(this.filePath);
		FileSystem file = FileSystem.get(URI.create(filePath), this.conf);
		FileStatus testFileStatus = file.getFileStatus(path);
		FSDataInputStream fileInputStream = file.open(path);
		byte[] buffer = new byte[Integer.parseInt(String.valueOf(testFileStatus.getLen()))];
		fileInputStream.readFully(0, buffer);
		BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));
		
		String line = null;
		int numOfData = 0;
		int numOfFeature = 0;
		Specification spec = new Specification();
		while((line = reader.readLine()) != null){
			if(line.startsWith("@")){
				//TODO nominal features mapping
			}
			else if(line.startsWith("#")){
				//number of data
				numOfData = Integer.parseInt((line.substring(1, line.length() - 1)));
			}
			else if(line.startsWith("&")){
				//number of features
				numOfFeature = Integer.parseInt((line.substring(1, line.length() - 1)));
			}
			else{
				break;
			}
		}
		
		Instance[] data = new Instance[numOfData];
		int dataCnt = 0;
		do{
			String[] split = line.split(",");
			for(int i = 0; i < numOfFeature; i++){
				if(spec.isNominal(i)){
					
				}
				else{
					data[dataCnt].fillFeature(i, Double.parseDouble(split[i]));
				}
			}
		}while((line = reader.readLine()) != null);
		dataSet = new DataSet(data, spec);
	}
	
	public DataSet getDataSet(){
		return dataSet;
	}
}
