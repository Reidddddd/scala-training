package kerberos.kafka.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.Minutes
import scala.collection.mutable.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author ${Reid Chan}
 */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <bootstrapServers> <group> <topic>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "group.id" -> args(1),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "client.id" -> "spark-streaming",
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.kerberos.service.name" -> "kafka"
    )
    val topics = Array(args(2))
    val lines = KafkaUtils.createDirectStream(ssc,
                                              LocationStrategies.PreferConsistent,
                                              ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    val line = lines.map(record => record.value())
    val words = line.flatMap(_.split(" |,|\\."))
    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(1), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
