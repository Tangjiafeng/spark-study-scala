package cn.spark.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.SparkConf
import cn.spark.study.sql.UDAFunction_average
import cn.spark.study.sql.UDAFunction_average

object UDAFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local") 
        .setAppName("UDAFunction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    // 构造模拟数据
    val employees = Array("Leo, 3000", "Marry, 4500", "Jack, 3500", "Tom, 4000") 
    val employeesRDD = sc.parallelize(employees, 1) 
    val employeesRowRDD = employeesRDD.map { row => Row(row.split(",")(0), row.split(",")(1).trim().toLong) }
    val structType = StructType(Array(StructField("name", StringType, true), 
        StructField("salary", LongType, true)))
    val employeesDF = sqlContext.createDataFrame(employeesRowRDD, structType)
    employeesDF.registerTempTable("employees")
//    employeesDF.show()
    sqlContext.udf.register("my_average", new UDAFunction_average)
    sqlContext.sql("select my_average(salary) as average_salary from employees").show()
  }
}