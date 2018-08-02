package exactlyonce

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.Random


object ExactlyOnceProducerTransactionTest extends App {

  object kafka {
    val producer = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("transactional.id", "toto")

      import org.apache.kafka.clients.producer.KafkaProducer
      val p = new KafkaProducer[Nothing, String](props)
      p.initTransactions()
      p
    }
  }

  val test = Random
  //kafka.producer.beginTransaction()
  //  for (i <- 1 to 10) {
  //    val message = new ProducerRecord("abort1", s"""{"name": "${Random.alphanumeric take 10 mkString ("")}", "age": ${test.nextInt(99)}}""")
  //    kafka.producer.send(message)
  //  }
  //  kafka.producer.commitTransaction()
  //  kafka.producer.beginTransaction()
  //  for (i <- 1 to 10) {
  //    val message = new ProducerRecord("abort1", s"""{"name": "${Random.alphanumeric take 10 mkString ("")}", "age": ${test.nextInt(99)}}""")
  //    kafka.producer.send(message)
  //  }
  //  kafka.producer.abortTransaction()
  //  kafka.producer.beginTransaction()
  //  for (i <- 1 to 10) {
  //    val message = new ProducerRecord("abort1", s"""{"name": "${Random.alphanumeric take 10 mkString ("")}", "age": ${test.nextInt(99)}}""")
  //    kafka.producer.send(message)
  //  }
  //  kafka.producer.commitTransaction()

  for (i <- 1 to 1000000) {
    kafka.producer.beginTransaction()
    val abort = test.nextInt(100) > 50
    for (i <- 1 to 3) {
      val message = new ProducerRecord("abort", s"""{"abort": "$abort", "name": "${Random.alphanumeric take 10 mkString ("")}", "age": ${test.nextInt(99)}}""")
      kafka.producer.send(message)
      Thread.sleep(5)
    }
    if (abort) {
      kafka.producer.abortTransaction()
      println("abort transaction")
    } else {
      println("commit transaction")
      kafka.producer.commitTransaction()
    }
    println(System.currentTimeMillis())
    Thread.sleep(300)
    //println(message)
  }
  println("closing")
  kafka.producer.close()
  println("closed")
}

