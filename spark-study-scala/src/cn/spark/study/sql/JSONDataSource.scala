package cn.spark.study.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column

object JSONDataSource {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("LoadAndSave").setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sqlConext = new SQLContext(sparkContext)
    // 姓名，成绩
    val studentScoreDF = sqlConext.read.json("E:\\spark\\json\\students.json")
    
    // 姓名，年龄
    val studentInfoRDD = sparkContext.parallelize(
        Array(
        "{\"name\":\"Leo\", \"age\":18}", 
        "{\"name\":\"Marry\", \"age\":17}",
        "{\"name\":\"Jack\", \"age\":19}"), 3)
    val studentInfoDF = sqlConext.read.json(studentInfoRDD)
    
    // 注册临时表
    studentScoreDF.registerTempTable("t_score")
    studentInfoDF.registerTempTable("t_info")
    
    // 查询成绩大于80分的学生，并保留姓名
    val goodStudentDF = sqlConext.sql("select name, score from t_score where score > 80")
//    goodStudentDF.show()    
    val goodStudentNames = goodStudentDF.rdd.map(row => row(0)).collect()
//    goodStudentNames.foreach(name => println(name))
    
    // 根据大于80分的学生姓名查询学生信息
    var sql = "select name, age from t_info where name in ("
    for (i <- 0 to goodStudentNames.length - 2) {
      sql += "'" + goodStudentNames(i) + "',"
    }
    sql += "'" + goodStudentNames(goodStudentNames.length - 1) + "')"
//    println(sql)
    val goodInfoDF = sqlConext.sql(sql)
//    goodInfoDF.show()
    val resultRDD = goodStudentDF.rdd.map(
        row => new Tuple2(row.getString(0), row.getLong(1).toInt))
    .join(goodInfoDF.rdd.map(row => new Tuple2(row.getString(0), row.getLong(1).toInt)))
    
    resultRDD.collect().foreach(p => println(
        "name:" + p._1 + "  " 
        + "score: " + p._2._1 + "  "
        + "age: " + p._2._2))
  }
}