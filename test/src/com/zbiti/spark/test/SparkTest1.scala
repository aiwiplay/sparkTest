package com.zbiti.spark.test
/**
 * 例子描述：

有个网站访问日志，有4个字段：（用户id，用户名，访问次数，访问网站）

需要统计：

1.用户的访问总次数去重

2.用户一共访问了多少种不同的网站

这里用sql很好写

select id,name,count(distinct url) from table group by id,name

其实这个题目是继官方和各种地方讲解聚合函数（aggregate）的第二个例子，第一个例子是使用aggregate来求平均数。

我们先用简易版来做一遍，后续我更新一份聚合函数版



原始数据：

id1,user1,2,http://www.baidu.com
id1,user1,2,http://www.baidu.com
id1,user1,3,http://www.baidu.com
id1,user1,100,http://www.baidu.com
id2,user2,2,http://www.baidu.com
id2,user2,1,http://www.baidu.com
id2,user2,50,http://www.baidu.com
id2,user2,2,http://www.sina.com

结果：
((id1,user1),4,1)
((id2,user2),4,2)
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DisFie").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val source =  sc.textFile("/wordcount/data2").collect()
    val RDD0 = sc.parallelize(source)

    RDD0.map {
      lines =>
        val line = lines.split(",")
        ((line(0), line(1)), (1, line(3)))
    }.groupByKey().map {
      case (x, y) =>
        val (n, url) = y.unzip
        (x, n.size, url.toSet.size)
    }.foreach(println)
  }
}