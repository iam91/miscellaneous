package com.zwy.core;

import java.util.Map;
import java.util.HashMap;

public class Specification {
	private Map<Integer, Map<String, Double>> nominal;
	
	public Specification(){
		this.nominal = new HashMap<Integer, Map<String, Double>>();
	}
	
	public void specify(int index, String[] valueSet){
		if(!nominal.containsKey(index)){
			Map<String, Double> map = new HashMap<String, Double>();
			double cnt = 0.0;
			for(String value: valueSet){
				cnt += 1.0;
				map.put(value, cnt);
			}
			nominal.put(index, map);
		}
	}
	
	public double getMappedValue(int index, String value){
		return nominal.get(index).get(value);
	}
	
	public boolean isNominal(int index){
		return nominal.containsKey(index);
	}
}
