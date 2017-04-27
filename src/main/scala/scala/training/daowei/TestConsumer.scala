package scala.training.daowei

import java.util.concurrent.TimeUnit

object TestConsumer {
  def main(args: Array[String]): Unit = {
    val config = new JobConfig
    config.setBatchInterval(2, TimeUnit.SECONDS)
    config.setGroupId("test")
    config.setNumThreads(2)
    config.setTopics("test-topic")
    config.setZkQuorum("hadoop-offline032.dx.momo.com:2181")

    val job = new StreamingJob(config, "type=3", 5)
    val result = job.computeStream(5, TimeUnit.MINUTES)
    print("Start time: " + result.getStartTime + "\n")
    print("End time: " + result.getEndTime + "\n")
    print("Type count: " + result.getTypeCount + "\n")
    print("Total count: " + result.getTotalTypeCount + "\n")
  }
}