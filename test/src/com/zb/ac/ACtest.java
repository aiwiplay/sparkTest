package com.zb.ac;

public class ACtest {
	public static void main(String[] args) {
		String url="m.sina.com";
		new AC2Util("/G:/tag_test.txt", ",");
		String tag=AC2Util.getTag(url);
		System.out.println(tag);
	}
}
