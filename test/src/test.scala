

object test {
  var strmethod1=(str1:String) => str1+"1232"
  def main(args: Array[String]): Unit = {
    print(123123)
    println()
    val args=Array("1","2")
    args.foreach((x:String)=>println(x))
    val list = "one" :: "two" :: "three" :: Nil
    println(list.count(p=>p.length()==3))
    
    var p1=(1,2)
    var p2=(p1,3)
    println(p2._1._2)
    var str="1"
    println(strmethod1(str))
    
//     val textFile = sc.textFile(args(0));
//     val wordCounts = textFile.flatMap(line => line.split(" ")).map( word => (word, 1)).reduceByKey((a, b) => a + b)
  }
  
  /*def methodTest1(str:String):String={
    return ""
  }*/
}