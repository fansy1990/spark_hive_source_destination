package hiveinout

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by fanzhe on 2016/11/27.
 * 调用方式：
 * spark-submit --class hiveinout.SparkReadWriteHive --master yarn --deploy-mode cluster --jars /usr/lib/hive/lib/datanucleus-core-3.2.10.jar --files /usr/lib/hive/conf/hive-site.xml spark_hive-1.0-SNAPSHOT.jar products tmp3
 */
object SparkReadWriteHive {

  def main(args:Array[String]): Unit ={
    if(args.length!=2){
      println("SparkReadHive: <table_in> <table_out>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("mySpark")
    val sc =new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val  data = sqlContext.table(args(0))
//    data.select(args(2).split(",").map(x => Column(x)))
//    data.insertInto(args(2)) // 废弃
    // data 可以是经过其他处理后的数据
    val tmpTableName = "tmp" + System.currentTimeMillis()
    data.registerTempTable(tmpTableName)
    sqlContext.sql("create table "+ args(1) +" as select * from "+ tmpTableName)
    sc.stop()
  }
}
