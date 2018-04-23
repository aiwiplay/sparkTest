package com.zb.ac;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import com.zb.ac.trie.Emit;
import com.zb.ac.trie.Trie;
import com.zb.common.TagParam;
import com.zb.util.ExcelUtil;
import com.zb.util.db.DBConnPool;
import com.zb.util.db.DBExecUtil;

public class AC2Util2 {
	public static ArrayList<TagParam> tagParams;
	public static String config_file;
	public static String config_file_regStr=",";
//	public static AhoCorasick tree;
	public static Trie trie = new Trie();
	public static java.util.List list1 = new ArrayList();
	public static Map<String, String> carTagConfigMap = new HashMap<String, String>();
	
	/*public AC2Util(String config_file,String config_file_regStr){
		this.config_file=config_file;
		this.config_file_regStr=config_file_regStr;
		this.readParamFile();//从配置文件中初始化tag标签库
//		this.readParamFileByMySQL();//从mysql中初始化tag标签库
//		this.readParamFileByExcel("/data/hb_test/gw_tag/conf/tag");
		this.buidTrie();
	}*/
	public AC2Util2(){
//		String path = AC2Util2.class.get
		this.config_file="./tag_conf.txt";
		this.config_file_regStr=",";
//		this.readParamFile();//从配置文件中初始化tag标签库
//		this.readParamFileByMySQL();//从mysql中初始化tag标签库
//		this.readParamFileByExcel("/data/hb_test/gw_tag/conf/tag");
//		list1.add("001,baidu.com,001");
//		list1.add("002,sina.com,002");
//		list1.add("003,www.baidu.com,003");
//		list1.add("004,qq.com,004");
		this.readParamByList(list1);
		this.buidTrie();
	} 
	
	public static void readParamByList(List<String> list) {
		try {
			String[] tmpStrs = null;
			tagParams = new ArrayList<TagParam>();
				//System.out.println("以行为单位读取文件内容，一次读一整行：");
				for(String tempString:list) {
					System.out.println("Line"  + ":" + tempString);
					tmpStrs = tempString.split(config_file_regStr);
					if(tmpStrs.length>=3){
						TagParam tagparam = new TagParam(tmpStrs[0], tmpStrs[1], tmpStrs[2]);
						tagParams.add(tagparam);
					}
				}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
//		return tagParams;
	}
	
	/**
	 * 初始化配置表
	 * @param filename
	 * @param regStr
	 */
	public static void readParamFile() {
		try {
			System.out.println("tag config file ----  ====> "+config_file);
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
					if(tmpStrs.length>=3){
						TagParam tagparam = new TagParam(tmpStrs[0], tmpStrs[1], tmpStrs[2]);
						tagParams.add(tagparam);
					}
					line++;
				}
				reader.close();
//				return tagParams;
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
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
//		return tagParams;
	}
	
	/**
	 * 初始化配置表
	 * @param filename
	 * @param regStr
	 */
	public static void readParamFileByMySQL() {
		try {
			System.out.println("tag init from mysql");
			tagParams = new ArrayList<TagParam>();
			DBConnPool dbConnPool = new DBConnPool();
			DBExecUtil execUtil = new DBExecUtil();
			Connection connection = dbConnPool.getConnection("mysql");
			String sql = "SELECT uli.label_name AS tag,ui.url AS url,ul.* " +
					"FROM ut_website_label ul LEFT JOIN ut_website_info ui " +
					"ON ul.website_id = ui.website_id " +
					"LEFT JOIN ut_label_library uli " +
					"ON ul.label_id = uli.label_id";
			List<Map<String, Object>> results = DBExecUtil.execQuery(connection, sql);
			/*for(Map<String, Object> map : results){
				System.out.println(map.get("TAG").toString()+"---->"+map.get("URL").toString()+"---->"+map.get("LABEL_ID").toString());
				if(map.get("TAG")!=null&&map.get("URL")!=null&&map.get("LABEL_ID")!=null){
					String tag_name=map.get("TAG").toString();
					String url = map.get("URL").toString();
					String tag_id = map.get("LABEL_ID").toString();
					if(!"".equals(tag_name)&&!"".equals(url)&&!"".equals(tag_id)){
						TagParam tagparam = new TagParam(map.get("TAG").toString(), map.get("URL").toString(), map.get("LABEL_ID").toString());
						tagParams.add(tagparam);
					}
				}
			}*/
			/*Map<String, Object> map = new HashMap<String, Object>();
			TagParam tagparam = new TagParam("qqQzone", "qzone.qq.com", "qqQzone");
			tagParams.add(tagparam);
			TagParam tagparam1 = new TagParam("taobaoCart", "cart.taobao.com", "taobaoCart");
			tagParams.add(tagparam1);
//			buyertrade.taobao.com
			TagParam tagparam2 = new TagParam("buyertrade", "buyertrade.taobao.com", "buyertrade");
			tagParams.add(tagparam2);*/
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
//		return tagParams;
	}
	
	
	/**
	 * 建立搜索树
	 * @param infileName
	 */
	public static void buidTrie(){
		//准备搜索树

		int length = tagParams.size();
		for (int i = 0; i < length; i++) {
			if (tagParams.get(i).getUrl().length() != 0) {
				trie.addKeyword(tagParams.get(i).getUrl(), tagParams
						.get(i).getTagname().trim());
			}
		}
	}
	
	/**
	 * 获取tag
	 * @return
	 */
	public static String getTag(String content){
		String tagStr="";
		Collection<Emit>  findResult = null;
		Iterator iret = null;
		findResult = trie.parseText(content);
		if (findResult.size() > 0) {
			iret = findResult.iterator();
			while (iret.hasNext()) {
				Emit obj1 = (Emit)iret.next();
				//System.out.println(obj1.toString());
				if (tagStr.equalsIgnoreCase("")) {
					tagStr = obj1.getLabkey();
				} else{
					if(!tagExist(tagStr,obj1.getLabkey())){
						tagStr = tagStr + "," + obj1.getLabkey();
					}
				}
			}
		}
//			tempString = line + "|" + tagStr + "|" + tempString;
		
		return tagStr;
	}
	/**
	 * 检查是否重复打标签
	 * @param tag
	 * @param one_tag
	 * @return
	 */
	public static boolean tagExist(String tag,String one_tag){
		//检测是否已经打了标签
		boolean tagExist=false;
		String[] tmpTagStr=tag.split(",");
		for(int i=0;i<tmpTagStr.length;i++){
			if(one_tag.equals(tmpTagStr[i])){
				tagExist=true;
			}
		}
		return tagExist;
	}
	
	/**
	 * 读一个文件夹里所有excel
	 * @param filePath
	 */
	public static void readParamFileByExcel(String filePath){
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(filePath+"/tag_id.txt"),true));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//获取目录文件下所有excel
		System.out.println("tag init from excels");
		tagParams = new ArrayList<TagParam>();
		File configPath = new File(filePath);
		File[] files = configPath.listFiles();
		List<String> fileNameLists = new ArrayList<String>();
		for(File file : files){
//			System.out.println(file.getName().replace("~$", "").replace(".xlsx", ""));
			String fileName = file.getName();
			String fileStuff = fileName.substring(fileName.lastIndexOf(".")+1);
			if("xlsx".equals(fileStuff)){
				fileNameLists.add(file.getName().replace("~$", ""));
			}
		}
		for(String filename : fileNameLists){
			try {
				System.out.println(filename);
				String fileNameWithOutStuff = filename.replace(".xlsx", "");
//				if(!carTagConfigMap.containsKey(fileNameWithOutStuff)){
//					continue;
//				}
//				String tagId = carTagConfigMap.get(fileNameWithOutStuff);
				String tagId=fileNameWithOutStuff;
				 InputStream is2 = new FileInputStream(filePath+"/"+filename);
		         Map<Integer, String> map = new ExcelUtil().readExcelContent(is2);
		         System.out.println("获得Excel表格的内容:");
		         for (int i = 1; i <= map.size(); i++) {
		        	String[]  arr = map.get(i).split("\t");
		        	TagParam tagparam = new TagParam(tagId+arr[0].trim().replace(".0", ""), arr[2].trim(), tagId+arr[0].trim().replace(".0", ""));
		        	System.out.println(tagId+arr[0].trim().replace(".0", "")+"--->"+arr[2]);
		        	String content = tagId+arr[0].trim().replace(".0", "")+"|"+arr[1].trim()+"|"+arr[2].trim();
		        	bw.write(content);
		        	bw.write("\n");
					tagParams.add(tagparam);
		         }
			} catch (Exception e) {
				// TODO: handle exception  
				e.printStackTrace();
			}
			
		}
		try {
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		/*File configPath = new File("G:/tmp/carData");
		File[] files = configPath.listFiles();
		for(File file : files){
			String fileName = file.getName();
			System.out.println(fileName);
//			String newFileName = carTagConfigMap.get(fileName.replace("~$", "").replace(".xlsx", ""))+".xlsx";
//			file.renameTo(new File("G:/tmp/carData/"+newFileName));
//			System.out.println(fileName+"===>"+newFileName);
		}*/
		/*String fileName = "aaa.xlsx";
		String fileStuff = fileName.substring(fileName.lastIndexOf(".")+1);
		System.out.println(fileStuff);*/
		
		readParamFileByExcel("G:/tmp/carData");
	}
}
