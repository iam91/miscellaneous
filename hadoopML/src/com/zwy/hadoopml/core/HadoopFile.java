package com.zwy.hadoopml.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.DataInputStream;
import java.net.URI;

public class HadoopFile {
	private Path filePath;
	private FSDataInputStream in;
	private FileSystem file;
	
	public HadoopFile(String path, Configuration conf) throws IOException{
		filePath = new Path(path);
		file = FileSystem.get(URI.create(path), conf);
		in = file.open(filePath);
	}
	
	public DataInputStream getInputStream(){
		return in;
	}
	
	public void closeFile() throws IOException{
		file.close();
	}
	
	public void deleteFile() throws IOException{
		file.deleteOnExit(filePath);
		file.close();
	}
}
