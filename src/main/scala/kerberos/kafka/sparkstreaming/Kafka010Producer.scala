package kerberos.kafka.sparkstreaming

import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import scala.util.Random
import org.apache.kafka.clients.producer.KafkaProducer

object Kafka010Producer {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: Kafka010Producer <brokersList> <topic> <messagePerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "spark-streaming")
    props.put("sasl.kerberos.service.name", "kafka");
    val producer = new KafkaProducer[String, String](props)

    var c = 0
    while (c < 300) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => Random.nextInt(10).toString()).mkString(" ")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
      c += 1;
    }
  }
}