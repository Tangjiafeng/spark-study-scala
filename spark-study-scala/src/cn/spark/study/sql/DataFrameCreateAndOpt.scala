package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataFrameCreateAndOpt {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
    val sc = new SparkContext(conf)    
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("E:\\spark\\json\\people.json")
//    val df = sqlContext.read.parquet("E:\\spark\\parquet\\users.parquet")
    df.show()
    
    df.select(df.col("name")).show()
    df.filter(df.col("age") > 18).show()
    df.groupBy("age").count().show()
    df.groupBy(df.col("age")).count().show()
  }
}