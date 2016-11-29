package demo

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fanzhe on 2016/11/27.
 */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local")
    val sc =new SparkContext(conf)
    val rdd =sc.parallelize(List(1,2,3,4,5,6)).map(_*3)
    val mappedRDD=rdd.filter(_>10).collect()
    //对集合求和
    println(rdd.reduce(_+_))
    //输出大于10的元素
    for(arg <- mappedRDD)
      print(arg+" ")
    println()
    println("math is work")
  }
}
