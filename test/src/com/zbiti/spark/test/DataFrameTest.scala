package com.zbiti.spark.test

import org.apache.spark.sql.{DataFrame, SQLContext}  
import org.apache.spark.{SparkConf, SparkContext}  

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    //设置环境  
    val conf=new SparkConf().setAppName("tianchi").setMaster("local")  
    val sc=new SparkContext(conf)  
    val sqc=new SQLContext(sc)  
  
    case class user_pay_class(shop_id:String,user_id:String,DS:String)//注册一个类  
  
//    val user_pay_raw=sc.textFile("G:/tmp/jptest/data.txt")  
    val user_pay_raw=sc.textFile(args(0)) 
    val user_pay_split=user_pay_raw.map(_.split(","))  
    val user_transform =user_pay_split.map{ x=>    //数据转换  
      val userid=x(0)  
      val shop_id=x(1)  
      val ts=x(2)  
      val ts_split=ts.split(" ")  
      val year_month_day=ts_split(0).split("-")  
      val year=year_month_day(0)  
      val month=year_month_day(1)  
      val day=year_month_day(2)  
//      (shop_id,userid,year,month,day)  
      (shop_id,userid,ts_split(0))  
    }  
    val df=sqc.createDataFrame(user_transform)  // 生成一个dataframe  
    val df_name_colums=df.toDF("shop_id","userid","DS")  //给df的每个列取名字  
    df_name_colums.registerTempTable("user_pay_table")     //注册临时表  
//    val sql="select shop_id,count(userid),DS from user_pay_table group by shop_id,DS order by shop_id desc,DS"  
    val sql="select shop_id,count(userid) from user_pay_table group by shop_id order by shop_id" 
    val rs: DataFrame =sqc.sql(sql)  
    rs.foreach(x=>println(x))  
//    user_transform.saveAsTextFile("/home/wangtuntun/test_file4.txt")  
//    val rs_rdd=rs.map( x=>( x(0),x(1),x(2) ) )  
    println("--------------yyyyasasassadsad------------------------")
    val rs_arr=rs.collect() //rs转为rdd
    val rs_rdd = sc.parallelize(rs_arr).map(z=>( z(0),z(1)))
    rs_rdd.foreach(y=>println(y))
    rs_rdd.saveAsTextFile(args(1))
//    rs_rdd.saveAsTextFile("G:/tmp/jptest/data1")  
    sc.stop();  
  }
}