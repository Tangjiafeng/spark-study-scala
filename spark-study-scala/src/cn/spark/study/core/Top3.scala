package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Top3 {
  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\Spark\\txt\\top.txt", 1)
    val pairs = lines.map { line => (line.toInt, line) }
    val sortedPairs = pairs.sortByKey(false, 1)
    val top3 = sortedPairs.take(3)
    top3.foreach( t => println(t._2) )
  }
}