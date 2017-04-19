package exactlyonce

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random


object ExactlyOnceProducerTest extends App{

  object kafka {
    val producer = {
      val props = new Properties()
      props.put("metadata.broker.list", "localhost:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")

      val config = new ProducerConfig(props)
      new Producer[String, String](config)
    }
  }
  for (i <- 1 to 1000000000) {
    val message =  new KeyedMessage[String, String]("exactlyonce", Random.nextString(30))
    kafka.producer.send(message)
    Thread.sleep(100)
  }
}

