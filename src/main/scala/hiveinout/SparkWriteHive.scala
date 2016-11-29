package hiveinout

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by fanzhe on 2016/11/27.
 * 调用方式：
 * spark-submit --class hiveinout.SparkWriteHive --master yarn --deploy-mode cluster --jars /usr/lib/hive/lib/datanucleus-core-3.2.10.jar --files /usr/lib/hive/conf/hive-site.xml spark_hive-1.0-SNAPSHOT.jar "create table tmp2 as select product_id,product_name from products"
 */
object SparkWriteHive {

  def main(args:Array[String]): Unit ={
    if(args.length!=1){
      println("SparkReadHive: <sql>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("mySpark")
    val sc =new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

//    val  data = sqlContext.table(args(0))
//    data.select(args(2).split(",").map(x => Column(x)))
    sqlContext.sql(args(0) )
    sc.stop()
  }
}
