package scala.training.daowei

import org.apache.spark.streaming.Duration
import java.util.concurrent.TimeUnit
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import kafka.common.InvalidConfigException

class JobConfig {
  private var batchDuration: Duration = Seconds(2)
  private var windowDuration: Duration = Seconds(10)
  private var slideDuration: Duration = Seconds(2)

  private var zkQuorum: String = ""
  private var groupId: String = ""
  private var topics: String = ""
  private var numThreads: Int = 1

  def setZkQuorum(quorum: String): Unit = {
    zkQuorum = quorum
  }

  def setTopics(tpcs: String): Unit = {
    topics = tpcs
  }

  def setGroupId(gid: String): Unit = {
    groupId = gid
  }

  def setNumThreads(num: Int): Unit = {
    numThreads = num
  }

  def setBatchInterval(duration: Long, unit: TimeUnit): Unit = {
    batchDuration = getDuration(duration, unit)
  }

  def setWindowDuration(duration: Long, unit: TimeUnit): Unit = {
    windowDuration = getDuration(duration, unit)
  }

  def setSlideDuration(duration: Long, unit: TimeUnit): Unit = {
    slideDuration = getDuration(duration, unit)
  }

  private def getDuration(duration: Long, unit: TimeUnit): Duration = {
    unit match {
      case TimeUnit.MINUTES => Minutes(duration)
      case TimeUnit.SECONDS => Seconds(duration)
      case _ => throw new InvalidConfigException("Wrong unit " + unit +
                                                 " of batch interval. Support Minutes and Seconds only.")
    }
  }

  def getZkQuorum: String = zkQuorum

  def getTopicMap: Map[String, Int] = {
    topics.split(",").map((_, numThreads.toInt)).toMap
  }

  def getGroupId: String = groupId

  def getNumThreads: Int = numThreads

  def getBatchDuration: Duration = batchDuration

  def getWindowDuration: Duration = windowDuration

  def getSlideDuration: Duration = slideDuration
}