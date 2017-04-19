package exactlyonce


import com.datastax.spark.connector._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}


object ExactlyOnceTestKillOnExecutorError extends App {
  val log = Logger.getLogger(getClass.getName)
  // configure the number of cores and RAM to use
  val conf = new SparkConf()
    .setMaster("local[5]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.executor.memory", "1G")
    .setAppName("exactly-once")

  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  val ssc = new StreamingContext(sc, Seconds(5))

  // configure kafka connection and topic
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

  def getFromOffsets(): Map[TopicAndPartition, Long] = {
    Map[TopicAndPartition, Long](TopicAndPartition("exactlyonce", 0) -> 0L)
  }
  val t: Map[TopicAndPartition, Long] = getFromOffsets()

  val messageHandler: MessageAndMetadata[String, String] => Tuple2[String, String] = mmd => (mmd.key, mmd.message)
  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Tuple2[String, String]](ssc, kafkaParams, getFromOffsets(), messageHandler)

  kafkaStream.transform{ rdd =>
    println(s"-------------------------")
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach{range => println(s"OFFSET RANGES: p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]")}
    rdd
  }.foreachRDD(rdd => {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach{range => println(s"SAVING RESULT TO C*: p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]")}
    rdd.saveToCassandra("exactlyonce", "output")
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach{range => println(s"RESULT SAVED TO C* : p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]")}
  })
  ssc.start()

  //ssc.awaitTermination()
  Try{
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

