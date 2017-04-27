package scala.training.daowei

import org.apache.spark.SerializableWritable

@SerialVersionUID(67890L)
class RDDResult (
    var typeCount: Long,
    var totalTypeCount: Long) extends Serializable {

  def getTypeCount: Long = {
    typeCount
  }

  def getTotalTypeCount: Long = {
    totalTypeCount
  }

  def setTypeCount(count: Long): Unit = {
    typeCount = count
  }

  def setTotalTypeCount(count: Long): Unit = {
    totalTypeCount = count
  }
}