package com.zbiti.spark.wp

import com.zb.ac.AC2Util2
import collection.JavaConverters._
object TagConfInit {
  def initAcUtil(tag_conf_list:Seq[String]): Unit = {
     var list: java.util.List[String] = tag_conf_list.asJava
     AC2Util2.list1 = list
     new AC2Util2()
  }
}