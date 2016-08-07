package com.zwy.hadoopml.logisticregression;

public class ArrayStringTrans {
	public static String doubleArrayToString(double[] array){
		String ret = "";
		for(int i = 0; i < array.length; i++){
			ret += (String.valueOf(array[i]) + (i < array.length - 1 ? "," : ""));
		}
		return ret;
	}
	
	public static double[] stringToDoubleArray(String s){
		String[] split = s.split(",");
		double[] ret = new double[split.length];
		for(int i = 0; i < split.length; i++){
			ret[i] = Double.parseDouble(split[i]);
		}
		return ret;
	}
}
