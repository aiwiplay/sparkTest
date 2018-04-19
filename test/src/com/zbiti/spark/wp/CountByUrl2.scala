package com.zbiti.spark.wp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}  
import com.zb.ac.AC2Util
import java.util.ArrayList

object CountByUrl2 {
  new AC2Util("G:/tmp/jptest/tag_conf.txt", ",")
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setAppName("CountUsersByUrl").setMaster("local")  
    val sc=new SparkContext(conf)  
    val sqc=new SQLContext(sc)
    case class user_tag_class(mdn:String,url:String,tag:String)//注册一个类
    
    val data=sc.textFile("G:/tmp/jptest/data2.txt") 
    var a = data.flatMap{line=>
      val x = line.split("\\|")
      if (x.length == 1) None
      else {
        println("x===>" + x.length + "---" + x(0))
        var mdn = ""
        if (x.length > 0) {
          //         mdn ==  x(1)
          mdn = x(0)
        }
        var url = ""
        if (x.length > 1) {
          //         url ==  x(29)
          url = x(1)
        }

        val tags = AC2Util.getTag(url).replace(",", "|")
        println("===>" + mdn + "---" + url + "---" + tags)
//        var x1 = Array(Array(mdn, url),tags)
        var x1=List(List(mdn,url), tags)
         var tmpList=(mdn,url)::Nil
         var list = (tmpList,tags)::Nil
        list
      }
    }
    
    val b = a.flatMapValues(_.split("\\|")).map(x=>{
      var mdn=x._1(0)._1
      var url=x._1(0)._2
      var tag=x._2
      (mdn,url,tag)
    })
b.collect.foreach(println(_))


val df=sqc.createDataFrame(b)  // 生成一个dataframe  
    val df_name_colums=df.toDF("mdn","url","tagid")  //给df的每个列取名字
    df_name_colums.registerTempTable("user_pay_table")     //注册临时表 
    var sql:String = "select tagid,count(distinct mdn) from user_pay_table group by tagid"
    val rs: DataFrame =sqc.sql(sql)  
    rs.foreach(x=>println(x))  
//    user_transform.saveAsTextFile("/home/wangtuntun/test_file4.txt")  
//    val rs_rdd=rs.map( x=>( x(0),x(1),x(2) ) )  
    println("--------------yyyyasasassadsad------------------------")
    val rs_arr=rs.collect() //rs转为rdd
    val rs_rdd = sc.parallelize(rs_arr).map(z=>( z(0),z(1)))
    rs_rdd.foreach(y=>println(y))
//    var b = sc.parallelize(a.collect())
//   b.flatm
    sc.stop();  
  }
}