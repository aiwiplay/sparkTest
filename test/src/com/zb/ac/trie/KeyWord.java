package com.zb.ac.trie;

public class KeyWord implements Comparable<KeyWord> {

	private String keyword;
	private String labkey;
	public String getKeyword() {
		return keyword;
	}
	public String getLabkey() {
		return labkey;
	}
	
	
	public KeyWord(String keyword, String labkey)
	{
		this.keyword = keyword;
		this.labkey = labkey;
	}
	@Override
	public int compareTo(KeyWord arg0) {
		return keyword.compareTo(arg0.getKeyword());
	}
	
	public String toString()
	{
		return this.getKeyword();
	}
}
