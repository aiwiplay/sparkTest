package com.zbiti.spark.test

import scala.tools.nsc.doc.model.Def
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ForTest {
  def main(args: Array[String]): Unit = {
    
    /* val str = List("1|a1#a9,2|b1#b9,3|c1#c9,4|d1#d8", "1|a2#a8,2|b2#b8,3|c2#c8,4|d2d8")
    println(str)
    var list1=("","")::Nil
    list1=("1", "12")::Nil
     var list2=("2", "22")::Nil
     list1 = list1 ::: list2
     println("listTest1=="+list1)
      println("list1=="+list1)
      println("list2=="+list2)

      list1.map{x=>
      println(x)
     }

    case class user_tag_class(mdn: String, tag: String) //注册一个类
    val rdd = str.map { x =>
      println(x)
      val arr1: Array[String] = x.split(",")
      var list = Nil
      val nums = 1::2::3::4::Nil
      for (i <- 0 to arr1.length - 1) {
        println("arr1(i)==>"+arr1(i))
        val arr2 = arr1(i).split("\\|")
        var tmpList = (arr2(0), arr2(1))::Nil
        println("tmpList===>"+tmpList)
        tmpList ::: list

      }
      println(list.size)
      list

    }
    println("-----------------------rdd-------------------------")
    rdd.map { x =>
      println(x)
    }*/

    test3();

  }

  def test(): Unit = {
    var list = List((("a1", "a2", "a3"), ("b1", "b2", "b3"), ("c1", "c2", "c3")), (("a11", "a22", "a33"), ("b11", "b22", "b33"), ("c11", "c22", "c33")), (("a1", "a2", "a3"), ("b1", "b2", "b3"), ("c1", "c2", "c3")), (("a11", "a22", "a33"), ("b11", "b22", "b33"), ("c11", "c22", "c33")))
    println(list.toArray)
    var arr1 = list.toArray
    
    val conf = new SparkConf().setAppName("CountUsersByUrl").setMaster("local")
    val sc = new SparkContext(conf)
//    var rdd = sc.makeRDD(list)
//    sc.parallelize(list).flatMap(x=>(x._1+x._2)).foreach(println)
    val arr2=sc.parallelize(Array(("A",1),("B",2),("C",3)))
    val arr=sc.parallelize(list)
//    var rdd = list.flatMap(x=>x._1)
//    val a = sc.parallelize(List((1,2),(3,4),(5,6)))
//    val b = a.flatMapValues(x=>1 to x)
//    b.collect.foreach(println(_))
    
//    val arr=sc.parallelize(Array(("A",1,),("B",2),("C",3)))
//    arr.flatmap(x=>(x._1+x._2)).foreach(println)
  }

  def test2(): Unit = {
    val sqlContext = SparkSession.builder().master("local").getOrCreate().sqlContext;
    val sparkContext = sqlContext.sparkContext
//    var a = Array((Array("1", "fruit"), "apple,banana,pear,jwb"), (Array("2", "animal"), "pig,cat,dog,tiger"))
    val a = sparkContext.parallelize(Array((Array("1", "fruit"), "apple,banana,pear,jwb"), (Array("2", "animal"), "pig,cat,dog,tiger")))
    val b = a.flatMapValues(_.split(",")).map(ele => {
      val num = ele._1(0)
      val name = ele._1(1)
      val cate = ele._2
      (num, name, cate)
    })
    import sqlContext.implicits._
    b.toDF("num", "name", "cate").show()
  }
  
  def test3(): Unit = {
    val s = Array("cai","yong") 
    println(s)
  }
}