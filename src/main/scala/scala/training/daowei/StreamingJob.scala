package scala.training.daowei

import java.util.concurrent.TimeUnit
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

class StreamingJob(
  @transient val conf: JobConfig,
  val wantedType: String,
  val expectedNum: Long) extends Serializable {

  @transient private val waitUnit = TimeUnit.MILLISECONDS
  @transient private val sparkConf = new SparkConf().setAppName("TypeFrequency")

  def computeStream(time: Long, unit: TimeUnit): Result = {
    val startTime = System.currentTimeMillis();
    val duration = toMillis(time, unit)
    val endTime = startTime + duration
    val result = new Result(startTime, endTime, 0, 0)

    val ssc = new StreamingContext(sparkConf, conf.getBatchDuration);
    val lines = KafkaUtils.createStream(ssc,
                                        conf.getZkQuorum,
                                        conf.getGroupId,
                                        conf.getTopicMap).map(_._2)
    val caches = lines.cache()
    val containType= (str: String) => {
      str.contains(wantedType)
    }
    caches.foreachRDD(
      cache => {
        val totalType = cache.count()
        val expectedType = cache.filter(containType(_)).count()
        result.addRDDResult(new RDDResult(expectedType, totalType))
        if (result.getTypeCount == expectedNum) {
          result.setEndTime(System.currentTimeMillis())
          ssc.stop(true)
        }
      }
    )
    ssc.start()
    if (ssc.awaitTerminationOrTimeout(duration)) {
    }
    result
  }

  private def toMillis(sourceDuration: Long, sourceUnit: TimeUnit): Long = {
    waitUnit.convert(sourceDuration, sourceUnit)
  }
}