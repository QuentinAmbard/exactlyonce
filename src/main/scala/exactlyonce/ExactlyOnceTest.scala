package exactlyonce

import com.datastax.spark.connector._
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration


object ExactlyOnceTest extends App{

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
  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("exactlyonce"))

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
  ssc.awaitTermination()
}

