package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column

object LoadAndSave {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("LoadAndSave").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sqlConext = new SQLContext(sparkContext)
    
    val studentDF = sqlConext.read.json("E:\\spark\\json\\students.json")
    // windows平台读取parquet文件会报错
//    val userDF = sqlConext.read.load("E:\\spark\\parquet\\users.parquet")
    studentDF.show()
//    userDF.show()
    studentDF.select(studentDF.col("score")).show()
    // save在windows平台会报错
//    studentDF.select(new Column("name")).write.save("E:\\spark\\json\\students_name.json")
  }
}