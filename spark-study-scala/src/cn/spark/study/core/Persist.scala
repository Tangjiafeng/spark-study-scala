package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Persist {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("persist").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\Spark\\txt\\big.txt", 1).persist()// RDD 持久化节省大量重复操作的时间
    val beginTime = System.currentTimeMillis();
  	val count = lines.count();
  	val endTime = System.currentTimeMillis();
  	println("cost times: " + (endTime - beginTime) + " : " + count)
  	val beginTime2 = System.currentTimeMillis();
  	val count2 = lines.count();
  	val endTime2 = System.currentTimeMillis();
  	println("cost times: " + (endTime2 - beginTime2) + " : " + count2)
  }
  
  
}