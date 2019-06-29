package cn.spark.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object Rdd2DataFrameProgrammatical extends App {
    val conf = new SparkConf().setAppName("DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    val rowRDD = sc.textFile("F:\\temp\\students.txt", 1).map(line => {
      Row(line.split(",")(0).toString(), line.split(",")(1).toString().toInt)
    })
    
    val structType = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))
    
    val stuDF = sqlContext.createDataFrame(rowRDD, structType)
    
//    stuRDD.show()
    
    stuDF.registerTempTable("student")
    
    val teenagerDF = sqlContext.sql("select * from student where age > 18")// sql 语句后不能加分号
    
    val teenagerRDD = teenagerDF.rdd.collect().foreach(f => println(f))
    
    
}