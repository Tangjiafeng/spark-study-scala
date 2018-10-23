package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("SecondSort")  
        .setMaster("local")
    val sc = new SparkContext(conf)
  
    val lines = sc.textFile("D:\\Spark\\txt\\sort.txt", 1)
    val pairs = lines.map { line => (
        new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line)}
    val sortedPairs = pairs.sortByKey(false)
    sortedPairs.foreach { sortedPair => println(sortedPair._2) }  
  }
}