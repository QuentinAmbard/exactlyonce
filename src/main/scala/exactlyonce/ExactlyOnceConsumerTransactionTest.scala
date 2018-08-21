package exactlyonce

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer


object ExactlyOnceConsumerTransactionTest extends App {

      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.deserializer", classOf[StringDeserializer])
      props.put("value.deserializer", classOf[StringDeserializer])
      props.put("group.id", "my_group")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000000")

  props.put("auto.offset.reset", "earliest")
      //props.put("isolation.level", "read_committed")
      props.put("enable.auto.commit", (true: java.lang.Boolean))
      val consumer = new KafkaConsumer[String, String](props)


  import collection.JavaConverters._

  private val topicPartition = new TopicPartition("abort", 0)
  consumer.assign(List(topicPartition).asJava)
  println(consumer.endOffsets(List(topicPartition).asJava))
  consumer.seekToEnd(List(topicPartition).asJava)
  println(consumer.position(topicPartition))
  consumer.seek(topicPartition, 0)
  while(true) {
    val records: ConsumerRecords[String, String] = consumer.poll(1000)
    for (record <- records.asScala) {
      println(s"${record.key()} - ${record.offset()} -  ${record.value()}")
    }
  }


//  for (i <- 1 to 100) {
//    println(i)
//    consumer.seek(topicPartition, i)
//    val records: ConsumerRecords[String, String] = consumer.poll(100)
//    for (record <- records.asScala) {
//      println(s"${record.key()} - ${record.offset()} -  ${record.value()}")
//    }
//  }


    println("closed")
}

