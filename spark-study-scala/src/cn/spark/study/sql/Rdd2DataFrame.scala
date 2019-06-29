package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Rdd2DataFrame extends App {
    val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    case class Student(name : String, age : Int)
    val lines = sc.textFile("F:\\temp\\students.txt", 1)
    
    val studentDF = lines.map(line => line.split(","))
    .map(arr => Student(arr(0).trim().toString, arr(1).trim().toInt))
    .toDF()
    
    studentDF.registerTempTable("student")
    
    val teenagerDF = sqlContext.sql("select * from student where age < 18")
    
    val teenagerRDD = teenagerDF.rdd
    
    teenagerRDD.map( row => Student(row(0).toString(), row(1).toString().toInt) )
        .collect().foreach(stu => println(stu.name + ":" + stu.age))

    teenagerRDD.map( row => Student(row.getAs[String]("name"),row.getAs[Int]("age")) )
    .collect().foreach(stu => println(stu.name + ":" + stu.age))
    
    val stuRDD = teenagerRDD.map( row => {
      val map = row.getValuesMap[Any](Array("name", "age"))
      Student(map("name").toString(), map("age").toString().toInt)
    } )
    
    stuRDD.collect().foreach(stu => println(stu.name + ":" + stu.age))
}