package com.zbiti.spark.test

import com.zb.ac.AC2Util

//import com.zb.ac.AC2Util

//import com.zb.ac.AC2Util
//import com.zb.ac.AC2Util
//import com.zb.ac.AC2Util
//import com.zb.ac.AC2Util


object AcTest {
  def main(args: Array[String]): Unit = {
    var url : String="www.baidu.comsina.com"
    new AC2Util("G:/tmp/jptest/tag_conf.txt", ",")
    val info= AC2Util.getTag(url)
    println(info)
  }
}