package com.zwy;

import com.zwy.core.FileReader;
import com.zwy.core.DataSet;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class Driver {
	public static void main(String[] args) throws IOException{
		String file = "/home/zwy/workspace/miscellaneous/data_banknote_authentication.csv";
		Configuration conf = new Configuration();
		FileReader reader = new FileReader(file, conf);
		DataSet dataSet = reader.getDataSet();
		System.out.println(dataSet.getData().length);
	}
}
