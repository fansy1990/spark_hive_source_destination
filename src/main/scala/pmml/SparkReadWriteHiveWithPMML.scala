package pmml

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}
import spark.{TransformerBuilder, EvaluatorUtil}

/**
 * Spark读取Hive表数据，调用PMML模型对表数据进行分析
 * 分析结果写入Hive表中
 * Created by fanzhe on 2016/11/30.
 * 调用方式：
 *
 */
object SparkReadWriteHiveWithPMML {

  def main(args:Array[String]): Unit ={
    if(args.length!=3){
      println("SparkReadHive: <table_in> <table_out> <pmml>")
      System.exit(1)
    }

    // create model
    val fs = FileSystem.get(new Configuration())
    val evaluator = EvaluatorUtil.createEvaluator(fs.open(new Path(args(2))))
    val transformer = new TransformerBuilder(evaluator).withTargetCols().withOutputCols().exploded(true).build()

    // read hive
    val conf = new SparkConf().setAppName("mySpark")
    val sc =new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val  data = sqlContext.table(args(0))
    val result = transformer.transform(data)
    // data 可以是经过其他处理后的数据
    val tmpTableName = "tmp" + System.currentTimeMillis()
    println("---->tmp:"+tmpTableName)
    result.registerTempTable(tmpTableName)
    sqlContext.sql("create table "+ args(1) +" as select * from "+ tmpTableName)
    sc.stop()
  }
}
