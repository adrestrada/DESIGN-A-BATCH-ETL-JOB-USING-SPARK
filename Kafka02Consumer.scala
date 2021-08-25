package ca.mcit.bigdata.project5

import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig._
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import scala.collection.JavaConverters._

object Kafka02Consumer extends App {

  val topicName = "bdsf2001_adriest_stop_times"
  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-1")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[Int, String](consumerProperties)
  consumer.subscribe(List(topicName).asJava)

  for (i <- 1 to 10) {
    val polledRecords: ConsumerRecords[Int, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator() //Java iterator
      while (recordIterator.hasNext) {
        val record: ConsumerRecord[Int, String] = recordIterator.next()
       val csvTrip = record.value()
      }
    }
  }
  consumer.close()
}
