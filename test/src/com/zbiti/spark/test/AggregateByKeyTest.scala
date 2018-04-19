package com.zbiti.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * select id,name,count(distinct url) from table group by id,name
 */
object AggregateByKeyTest {
  def main(args: Array[String]) {

    case class User(id: String, name: String, vtm: String, url: String)
    //val rowkey = (new RowKey).evaluate(_)
    val HADOOP_USER = "hdfs"
    // 设置访问spark使用的用户名
    System.setProperty("user.name", HADOOP_USER);
    // 设置访问hadoop使用的用户名
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER);

    val conf = new SparkConf().setAppName("wordcount").setMaster("local").setExecutorEnv("HADOOP_USER_NAME", HADOOP_USER)
    val sc = new SparkContext(conf)
    //    val data = sc.textFile("/wordcount/data2")
    val arr = Array(
      "id1,user1,2,http://www.baidu.com",
      "id1,user1,2,http://www.baidu.com",
      "id1,user1,3,http://www.baidu.com",
      "id1,user1,100,http://www.baidu.com",
      "id2,user2,2,http://www.baidu.com",
      "id2,user2,1,http://www.baidu.com",
      "id2,user2,50,http://www.baidu.com",
      "id2,user2,2,http://www.sina.com",
      "id3,user3,2,http://www.sina.com",
      "id2,user2,2,http://www.sina.com")
    val data = sc.parallelize(arr)
    val rdd1 = data.map(line => {
      val r = line.split(",")
      User(r(0), r(1), r(2), r(3))
    })

    /**
     *rdd2： ((id1,user1),User(id1,user1,2,http://www.baidu.com))
        ((id1,user1),User(id1,user1,2,http://www.baidu.com))
        ((id1,user1),User(id1,user1,3,http://www.baidu.com))
        ((id1,user1),User(id1,user1,100,http://www.baidu.com))
        ((id2,user2),User(id2,user2,2,http://www.baidu.com))
        ((id2,user2),User(id2,user2,1,http://www.baidu.com))
        ((id2,user2),User(id2,user2,50,http://www.baidu.com))
        ((id2,user2),User(id2,user2,2,http://www.sina.com))
     */
    val rdd2 = rdd1.map(r => ((r.id, r.name), r))
    println("-------------rdd2---------------")
    rdd2.collect().foreach(println)
    //::: 该方法只能用于连接两个List类型的集合
    //:: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。用法为 x::list,其中x为加入到头部的元素
    val seqOp = (a: (Int, List[String]), b: User) => a match {
      case (0, List()) => (1, List(b.url))
      case _=> (a._1 + 1, b.url :: a._2)

    }

    val combOp = (a: (Int, List[String]), b: (Int, List[String])) => {
      (a._1 + b._1, a._2 ::: b._2)
    }
/**
 * rdd3:
 * ((id1,user1),(4,List(http://www.baidu.com, http://www.baidu.com, http://www.baidu.com, http://www.baidu.com)))
((id2,user2),(4,List(http://www.sina.com, http://www.baidu.com, http://www.baidu.com, http://www.baidu.com)))
 */
    println("---------------rdd3--------------------------")
    val rdd3 = rdd2.aggregateByKey((0, List[String]()))(seqOp, combOp)
     rdd3.collect.foreach(println)
     println("---------------rdd4--------------------------")
     /**
      * ((id1,user1),4,1)
				((id2,user2),4,2)
      */
    val rdd4 =rdd3.map(a => {
      (a._1, a._2._1, a._2._2.distinct.length)
    })
    rdd4.collect.foreach(println)
   
    sc.stop()
  }
}