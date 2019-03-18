package exactlyonce

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestSimpleStreamTransaction extends App {

  val conf = new SparkConf().setAppName("exactly-once")
    .setMaster("local[5]")
    .set("spark.streaming.kafka.allowNonConsecutiveOffsets" ,"true")
    .set("spark.streaming.kafka.maxRatePerPartition", "50")

  val ssc = new StreamingContext(conf, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test",
    "isolation.level" -> "read_committed",
  //"transactional.id" -> "toto",
    "auto.offset.reset" -> "latest",
    ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "40",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  import org.apache.log4j.{Level, Logger}

  //Logger.getLogger(classOf[OffsetWithRecordScanner[_,_]]).setLevel(Level.TRACE)
  Logger.getLogger("org.apache.spark.streaming.kafka010.DirectKafkaInputDStream").setLevel(Level.TRACE)


  var size  = 0
  val topics = Array("abort")
  val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  kafkaStream.foreachRDD(rdd => {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach { range => println(s"SAVING RESULT: p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]") }  //(size: ${range.recordNumber}
    println(s"count = ${rdd.count()}")
    val count = rdd.aggregate(0)((a, string) => a +1, (a,b) => a+b)
    size = size + count
    rdd.foreach(println)
    println(s"TOTAL = $size. real count = ${count}")
  })
  ssc.start()
  ssc.awaitTermination()

}

