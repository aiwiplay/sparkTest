package com.zb.ac;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import com.zb.common.TagParam;


public class AC_dx_ip_tag_util {
	public static ArrayList<TagParam> tagParams;
	public static AhoCorasick tree;
	public static AcApply ac = new AcApply();
	public static String config_file;
	public static String config_file_regStr="\t";
	public AC_dx_ip_tag_util(String config_file,String config_file_regStr){
		this.config_file=config_file;
		this.config_file_regStr=config_file_regStr;
		this.readParamFile();
		this.buidTree();
	}
	
	/**
	 * 初始化配置表
	 * @param filename
	 * @param regStr
	 */
	public static void readParamFile() {
		File file = new File(config_file);
		BufferedReader reader = null;
		String tempString = null;
		int line = 1;
		String[] tmpStrs = null;
		tagParams = new ArrayList<TagParam>();
		try {
			//System.out.println("以行为单位读取文件内容，一次读一整行：");
			reader = new BufferedReader(new FileReader(file));
			while ((tempString = reader.readLine()) != null) {
				System.out.println("Line" + line + ":" + tempString);
				tmpStrs = tempString.split(config_file_regStr);
				TagParam tagparam = new TagParam(tmpStrs[0], tmpStrs[1], tmpStrs[2]);
				tagParams.add(tagparam);
				line++;
			}
			reader.close();
//			return tagParams;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
//		return tagParams;
	}
	
	/**
	 * 建立搜索树
	 * @param infileName
	 */
	public static void buidTree(){
//		File file = new File(infileName);
//		BufferedReader reader = null;
//		
//		FileWriter fw = null;

		String tempString = null;
		Set findResult = null;
		int line = 1,count = 1;
		Iterator iret = null;

		
		//准备搜索树

		tree = new AhoCorasick();
		int length = tagParams.size();
		for (int i = 0; i < length; i++) {
			if (tagParams.get(i).getUrl().length() != 0) {
				tree.add(tagParams.get(i).getUrl().trim().getBytes(), tagParams
						.get(i).getTagname().trim());
			}
		}
		tree.prepare();
	}
	/**
	 * 获取tag
	 * @return
	 */
	public String getTag(String content){
		String tagStr="";
		Set findResult = null;
		Iterator iret = null;
		findResult = ac.findWordsInArray(tree, content);
		if (findResult.size() > 0) {
			iret = findResult.iterator();
			while (iret.hasNext()) {
				Object obj1 = iret.next();
				//System.out.println(obj1.toString());
				if (tagStr.equalsIgnoreCase("")) {
					tagStr = obj1.toString();
				} else {
					tagStr = tagStr + "," + obj1.toString();
				}
			}
		}
		return tagStr;
	}
}
