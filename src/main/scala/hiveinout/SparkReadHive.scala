package hiveinout

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fanzhe on 2016/11/27.
 * 调用方式：
 * spark-submit --class hiveinout.SparkReadHive --master yarn --deploy-mode cluster --jars /usr/lib/hive/lib/datanucleus-core-3.2.10.jar --files /usr/lib/hive/conf/hive-site.xml spark_hive-1.0-SNAPSHOT.jar
 */
object SparkReadHive {

  def main(args:Array[String]): Unit ={
//    if(args.length!=2){
//      println("SparkReadHive: <table> <outupt>")
//      System.exit(1)
//    }
    val conf = new SparkConf().setAppName("mySpark")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//    conf.setMaster("local")
    val sc =new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val df = sqlContext.tables()
    df.show(10)
//    sqlContext.table(args(0))
    sc.stop()
  }
}
