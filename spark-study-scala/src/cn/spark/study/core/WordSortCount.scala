package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordSortCount {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("WordSortCount")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\Spark\\txt\\big.txt", 1)
    val words = lines.flatMap { line => line.split(" ") }
    val wordPair = words.map( word => (word, 1))
    val wordCount = wordPair.reduceByKey(_ + _)
    val word2Count = wordCount.map{ word => (word._2, word._1) }
    val wordSortCount = word2Count.sortByKey(false, 1)
    wordSortCount.foreach{ wsc => println(wsc._2 + " appears " + wsc._1 + " times.") }
  }
}