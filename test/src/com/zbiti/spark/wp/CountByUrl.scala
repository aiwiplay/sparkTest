package com.zbiti.spark.wp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}  
import com.zb.ac.AC2Util
import java.util.ArrayList

object CountByUrl {
  new AC2Util("G:/tmp/jptest/tag_conf.txt", ",")
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setAppName("CountUsersByUrl").setMaster("local")  
    val sc=new SparkContext(conf)  
    val sqc=new SQLContext(sc)
    case class user_tag_class(mdn:String,url:String,tag:String)//注册一个类
    
    val data=sc.textFile("G:/tmp/jptest/data2.txt") 
    val data_split=data.map(_.split("\\|"))  
    /**
     * ((18013807898,www.baidu.comsina.com,002),(18013807898,www.baidu.comsina.com,003),(18013807898,www.baidu.comsina.com,001))
       ((18013807899,www.baidu.comsina.comqq.com,004),(18013807899,www.baidu.comsina.comqq.com,002),(18013807899,www.baidu.comsina.comqq.com,003))
     */
     val data_transform =data_split.map{ x=>    //数据转换
       println("x===>"+x.length+"---"+x(0))
       var mdn=""
       if(x.length>0){
//         mdn ==  x(1)
          mdn =  x(0)
       }  
      var url=""
      if(x.length>1){
//         url ==  x(29)
        url =  x(1)
       }
      
       println("===>"+mdn+"---"+url)
      val tags=AC2Util.getTag(url)
      
//      (shop_id,userid,year,month,day)
      var list = ("","","")::Nil
      val tagArr:Array[String] = tags.split(",")
      for( i <- 0 to tagArr.length-1){
          var tmpList=(mdn,url,tagArr(i))::Nil
          list=tmpList ::: list
      }
      list
      /*var data =  list.map{y=>
        y
      }*/
//     (mdn,url,tags) 
    }  
    var a = data_transform.collect().map{y=>
      (y(0))
    }
    a.foreach(z=>println("---------"+z))
    
    val df=sqc.createDataFrame(a)  // 生成一个dataframe  
    val df_name_colums=df.toDF("mdn","url","tagid")  //给df的每个列取名字
    df_name_colums.registerTempTable("user_pay_table")     //注册临时表 
    var sql:String = "select tagid,count(1) from user_pay_table group by tagid"
    val rs: DataFrame =sqc.sql(sql)  
    rs.foreach(x=>println(x))  
//    user_transform.saveAsTextFile("/home/wangtuntun/test_file4.txt")  
//    val rs_rdd=rs.map( x=>( x(0),x(1),x(2) ) )  
    println("--------------yyyyasasassadsad------------------------")
    val rs_arr=rs.collect() //rs转为rdd
    val rs_rdd = sc.parallelize(rs_arr).map(z=>( z(0),z(1)))
    rs_rdd.foreach(y=>println(y))
//    rs_rdd.saveAsTextFile(args(1))
//    rs_rdd.saveAsTextFile("G:/tmp/jptest/data1")  
    sc.stop();  
//    data_transform.map{y=>
//      println(y)
//    }
    
    
//    data.co
//     val df=sqc.createDataFrame(data_transform)  // 生成一个dataframe  
//    sqc.createDataFrame(data_transform, user_tag_class)
//     
  }
}