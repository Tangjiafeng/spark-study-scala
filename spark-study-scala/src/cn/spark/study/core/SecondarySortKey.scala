package cn.spark.study.core

class SecondarySortKey(val first : Int, val second : Int) extends Ordered[SecondarySortKey] with Serializable {
  def compare(that: SecondarySortKey): Int = {
    if(this.first != that.first) {
      return this.first - that.first
    } else {
      this.second - that.second
    }
  }
}