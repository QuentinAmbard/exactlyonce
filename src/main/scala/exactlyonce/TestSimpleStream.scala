package exactlyonce

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestSimpleStream {

  val conf = new SparkConf().setAppName("exactly-once")
  val ssc = new StreamingContext(conf, Seconds(5))

  var kafkaHost = "localhost:9092"
  def main(args: Array[String]): Unit = {
    if(args.length>0) {
      kafkaHost = args(0)
    }
  }

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> kafkaHost,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  val topics = Array("exactlyonce")
  val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  kafkaStream.foreachRDD(rdd => {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach { range => println(s"SAVING RESULT: p:${range.partition} => [${range.fromOffset}-${range.untilOffset}]") }
    println(rdd.count())
    val count = rdd.aggregate(0)((a, string) => a +1, (a,b) => a+b)
    println(s"real count = ${count}")

  })
  ssc.start()
  ssc.awaitTermination()

  ssc.stop()

}

