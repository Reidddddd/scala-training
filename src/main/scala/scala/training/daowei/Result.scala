package scala.training.daowei

@SerialVersionUID(12345L)
class Result(
    @transient val startTime: Long,
    var endTime: Long,
    typeCount: Long,
    totalTypeCount: Long)
    extends RDDResult(typeCount, totalTypeCount) with Serializable {

  def getStartTime: Long = {
    startTime
  }

  def getEndTime: Long = {
    endTime
  }

  def setEndTime(time: Long): Unit = {
    endTime = time
  }

  def addRDDResult(rddRes: RDDResult): Unit = {
    setTypeCount(getTypeCount + rddRes.getTypeCount)
    setTotalTypeCount(getTotalTypeCount + rddRes.getTotalTypeCount)
  }
}