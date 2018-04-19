package com.zbiti.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/18 
  * Time: 16:44  
  */



object WordCountTest {
  def main(args: Array[String]) {
    /*if (args.length != 2) {
      System.err.println("Usage: SparkWordCount <input> <output>")
    }*/

    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    val file = sc.textFile("/user/u_tel_hlwb_mqj/private/test")
//    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("/user/u_tel_hlwb_mqj/private/test_result")
//      counts.saveAsTextFile(args(1))
    sc.stop()
  }

  def execute(input: String, output: String, master: Option[String] = Some("local")): Unit = {
    val sc = {
      val conf = new SparkConf().setAppName("Spark WordCount")
      for (m <- master) {
        conf.setMaster(m)
      }
      new SparkContext(conf)
    }

    // Adapted from Word Count example on http://spark-project.org/examples/
    val file = sc.textFile(input)
    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.saveAsTextFile(output)
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")

  }
}
