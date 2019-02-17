package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting
import java.util.Arrays

object GroupTop3 {
  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("GroupTop3")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\Spark\\txt\\score.txt", 1)
    val pairs = lines.map( line => {
        val p = line.split(" ")
        (p(0), p(1).toInt)
        } )
        
    val groupPairs = pairs.groupByKey()
    groupPairs.foreach{ pair => 
         {
            println(pair._1)
            println(pair._2)
            val scores = pair._2.iterator
            val arr = ArrayBuffer[Int]()
            while(scores.hasNext) {
              arr += scores.next()
            }
            val sortArr = arr.toArray
            Sorting.quickSort(sortArr)
            for(i <- 0 to 2) {
              println(sortArr(sortArr.length - i - 1))
            }
            
         }
      }    
  }
}