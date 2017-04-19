package exactlyonce


import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Random, Success, Try}


object ExactlyOnceTestForEachKill extends App {
  val log = Logger.getLogger(getClass.getName)
  // configure the number of cores and RAM to use
  val conf = new SparkConf()
    //.setMaster("local[5]")
    //.setMaster("spark://127.0.0.1:7077")
    //.set("spark.cassandra.connection.host", "localhost")
    //.set("spark.executor.memory", "1G")
    .setAppName("exactly-once")

  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = SparkSession.builder().config(conf)
  val ssc = new StreamingContext(sc, Seconds(1))

  // configure kafka connection and topic
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "largest")

  def getFromOffsets(): Map[TopicAndPartition, Long] = {
    Map[TopicAndPartition, Long](TopicAndPartition("exactlyonce", 0) -> 0L)
  }

  val t: Map[TopicAndPartition, Long] = getFromOffsets()

  val messageHandler: MessageAndMetadata[String, String] => Tuple2[String, String] = mmd => (mmd.key, mmd.message)
  //val kafkaStream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Tuple2[String, String]](ssc, kafkaParams, getFromOffsets(), messageHandler)

  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("exactlyonce"))

  kafkaStream.foreachRDD(rdd => {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach { range => println(s"SAVING RESULT TO C*: p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]") }
    val result = rdd.map(row => {
      Thread.sleep(Random.nextInt(100))
      row
    }).collect()
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach { range => println(s"RESULT COLLECTED FOR:  p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]") }
  })
  ssc.start()


  //ssc.awaitTermination()
  Try {
    ssc.awaitTermination()
  } match {
    case Success(result) =>
      if (ssc.getState().compareTo(StreamingContextState.STOPPED) != 0) {
        log.warn("AwaitTermination get notified but StreamingContext has not been stopped")
      }
      else {
        log.error(" StreamingContext has been stopped ")
      }
    case Failure(exception) =>
      log.error("Streaming terminated", exception)
      ssc.stop(stopSparkContext = true, stopGracefully = false)
      System.exit(50) // force kill the driver, same as SparkExitCode.UNCAUGHT_EXCEPTION
  }
}

