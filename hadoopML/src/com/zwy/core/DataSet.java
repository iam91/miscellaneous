package com.zwy.core;

public class DataSet {
	private Instance[] data;
	private Specification spec;
	
	public DataSet(Instance[] data, Specification spec){
		this.data = data;
		this.spec = spec;
	}
	
	public Instance[] getData(){
		return data;
	}
	
	public Specification getSpecification(){
		return spec;
	}
}
