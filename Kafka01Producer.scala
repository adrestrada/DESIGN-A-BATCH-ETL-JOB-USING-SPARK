package ca.mcit.bigdata.project5

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object Kafka01Producer extends App {

  val topicName = "bdsf2001_adriest_stop_times"

  val producerProperties = new Properties()
  producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](producerProperties)
  val dataSource = Source.fromFile("C:/Users/Valeria/STMdata/stop_times.txt")
  dataSource
    .getLines().slice(1, 101)
    .foreach(line => {
      producer.send(new ProducerRecord[String, String](topicName, line))
    })
  dataSource.close()
  producer.flush()
}