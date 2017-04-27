package scala.training.daowei

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties
import scala.util.Random
import org.apache.kafka.clients.producer.ProducerConfig

object StringTypeProducer {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: StringTypeProducer <brokersList> <topic> <messagePerSec> <typesPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, typesPerMessage) = args
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    var c = 0
    while (c < 300) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to typesPerMessage.toInt).map(x => "60.181.107.9 - - 05/may/2014:12:26:37 0800 GET /v.gif?pid=121&type=" +
                  Random.nextInt(10).toString() + "&mc=weapp").mkString("\n");
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
      c += 1;
    }
  }
}