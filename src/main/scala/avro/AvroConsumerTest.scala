package avro

import java.util.Properties
import scala.collection.JavaConversions._

object AvroConsumerTest extends App {

  import java.util

  import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

  val props = new Properties()

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1")


  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
  props.put("schema.registry.url", "http://localhost:8081")

  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val topic = "user"
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  try
      while (true) {
        val records = consumer.poll(100)
        println("empty")
        for (record <- records) {
          println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
        }
      }
  finally consumer.close()
}

