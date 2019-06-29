package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LineCount {
  def main(args : Array[String]) {
    val conf = new SparkConf()
        .setAppName("LineCount")
        .setMaster("local")
        val sc = new SparkContext(conf)
    
    val lines = sc.textFile("D:\\Spark\\txt\\hello.txt", 1)
    val linePairs = lines.map { line => (line, 1) }
    val lineCounts = linePairs.reduceByKey { _ + _ }
    lineCounts.foreach(lineCount => println(lineCount._1 + " appears " + lineCount._2 + " times."))
  }
}