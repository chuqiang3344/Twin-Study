package com.tyaer.hadoop.mr.utils;

public class IDGenerator {
	
	
	public static String generator(String... fields){
		StringBuilder sb = new StringBuilder();
		for(String field:fields){
			if(sb.length()>0){
				sb.append("|");
			}
			sb.append(field);
		}
		return Md5Utils.getMd5ByStr(sb.toString());
	}

}
