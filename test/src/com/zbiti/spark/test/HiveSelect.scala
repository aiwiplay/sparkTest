package com.zbiti.spark.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import java.io.FileNotFoundException
import java.io.IOException
import org.apache.spark.sql.SparkSession

import org.apache.spark._  
import org.apache.spark.sql._  
//import org.apache.spark.sql.types._  

object HiveSelect {
  def main(args: Array[String]) {
//      System.setProperty("hadoop.home.dir", "D:\\hadoop") //加载hadoop组件
      val conf = new SparkConf().setAppName("HiveApp").setMaster("yarn")
        .set("spark.executor.memory", "1g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//        .setJars(Seq("D:\\workspace\\scala\\out\\scala.jar"))//加载远程spark
        //.set("hive.metastore.uris", "thrift://192.168.66.66:9083")//远程hive的meterstore地址
      // .set("spark.driver.extraClassPath","D:\\json\\mysql-connector-java-5.1.39.jar")
      val sparkcontext = new SparkContext(conf);
      try {
        val hiveContext = new HiveContext(sparkcontext);
        hiveContext.sql("use db_lte"); //使用数据库
//        hiveContext.sql("DROP TABLE IF EXISTS src") //删除表
//        hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) " +
//          "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ");//创建表
//        hiveContext.sql("LOAD DATA LOCAL INPATH 'D:\\workspace\\scala\\src.txt' INTO TABLE src  "); //导入数据
//        hiveContext.sql("select * from (select a.msisdn as mdn,(case when a.url like '%.lagou.com%' then 'xyk0010060002'else when a.url like '%.51job.com%' then 'xyk0010060003'else '' end) as tag_id from sada_lte_ufer a where a.dt=20180416) b where b.tag_id !=''  limit 10;").collect().foreach(println);//查询数据
        hiveContext.sql("select * from sada_lte_ufer a where a.dt=20180416 limit 10;").collect().foreach(println);//查询数据
      }
      catch {
        case e: FileNotFoundException => println("Missing file exception")
        case ex: IOException => println("IO Exception")
        case ee: ArithmeticException => println(ee)
        case eee: Throwable => println("found a unknown exception" + eee)
        case ef: NumberFormatException => println(ef)
        case ec: Exception => println(ec)
        case e: IllegalArgumentException => println("illegal arg. exception");
        case e: IllegalStateException    => println("illegal state exception");
      }
      finally {
        sparkcontext.stop()
      }
    }
  
  
  case class Person(name:String,age:Int)
  def rddToDFCase(sparkSession : SparkSession):DataFrame=  {
    //导入隐饰操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val peopleRDD = sparkSession.sparkContext
      .textFile("file:/E:/scala_workspace/z_spark_study/people.txt",2)
      .map( x => x.split(",")).map( x => Person(x(0),x(1).trim().toInt)).toDF()
    peopleRDD
}
}