package exactlyonce

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object ExactlyOnceStructuredTestSimple extends App {
//
//  val sparkSession = SparkSession.builder
//    .master("local[2]")
//    .appName("testStructured")
//    .config("spark.task.maxFailures", "4")
//    .config("spark.executor.memory", "1G")
//    //.config("spark.hadoop.fs.ckfs.impl", "exactlyonce.CassandraSimpleFileSystem")
//    //.config("spark.hadoop.cassandra.host", "10.240.0.114")
//    .config("spark.cassandra.connection.host", "127.0.0.1")
//    .getOrCreate()
//
//
//  case class Person(name: String, age: Int) {
//    def this() = this(null, 0)
//  }
//
//
////  val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[Nothing]
////  root.setLevel(Level.INFO)
////
//  LogManager.getRootLogger.setLevel(Level.WARN)
//
//  import sparkSession.implicits._
//
//
//  val schema = StructType(Seq(
//    StructField("name", StringType, true),
//    StructField("age", IntegerType, true)
//  ))
//
//  import org.apache.spark.sql.functions.from_json
//
//  val ds = sparkSession.readStream
//    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
//    .option("kafka.bootstrap.servers", "localhost:9092")
//    .option("kafka.isolation.level", "read_committed")
//    .option("failOnDataLoss", "true")
//    .option("subscribe", "exactlyonce")
//    .option("maxOffsetsPerTrigger", "100")
//    .load()
//    .withColumn("value", $"value".cast(StringType))
//    .select(from_json($"value", schema).as("json")).select("json.*").as[Person]
////      .selectExpr("from_json(cast(value as string), 'age Integer, name String') as json").select("json.*").as[Person]
//
//
//  import scala.concurrent.duration._
//
//  //private val checkpointLocation = if(System.getenv("checkpointLocation") == null) "file:///home/quentin/Downloads/checkpoint1" else System.getenv("checkpointLocation")
//  private val checkpointLocation = "file:///home/quentin/Downloads/checkpoint6"
//
//  val query = ds.writeStream
//    //.trigger(Trigger.Once())
//    //.trigger(Trigger.ProcessingTime(1 seconds))
//    .outputMode(OutputMode.Append())
//    .format("console")
//    .option("checkpointLocation", checkpointLocation)
//    .queryName("test")
//  //.foreach(personWriter)
//
//  val sq = query.start()
//  //LogManager.getRootLogger.setLevel(Level.INFO)
//
//
//  sq.awaitTermination()

}
