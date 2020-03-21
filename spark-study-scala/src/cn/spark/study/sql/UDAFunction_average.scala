package cn.spark.study.sql

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType

class UDAFunction_average extends UserDefinedAggregateFunction {
  // 输入类型
  def inputSchema: StructType = {
	  StructType(StructField("inputColumn", LongType) :: Nil)
	}
  // 聚合数据类型
	def bufferSchema() : StructType = {
		StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
	}
  // The data type of the returned value
	def dataType() : DataType = {
	  DoubleType
	}
	// 此函数是否始终在相同的输入上返回相同的输出
	def deterministic() : Boolean =  true
	// 初始化聚合数据
  def initialize(buffer: MutableAggregationBuffer): Unit = {
	  buffer(0) = 0L// sum
	  buffer(1) = 0L// count
	}
	// 更新聚合数据
	def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
	    buffer(1) = buffer.getLong(1) + 1
    }
	}
	// 分布式系统，合并最终结果
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
	  buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
	  buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
	}
	// 最终结果，求平均值
  def evaluate(buffer: Row): Double = {
	  buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
	}  
}