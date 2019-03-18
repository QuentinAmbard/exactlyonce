package exactlyonce

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random


case class CustomSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import org.apache.spark.sql.functions._
    import data.sparkSession.implicits._
    val offsets = data.groupBy($"partition").agg(min($"offset").as("min_offset"), max($"offset").as("max_offset")).select("partition", "min_offset", "max_offset").collect()
    offsets.foreach(row => {
      println(s"Partition=${row.getAs[Int]("partition")} => [${row.getAs[Int]("min_offset")} -> ${row.getAs[Int]("max_offset")}]")
    })
    //create table offsets (jobName text, topicName text, partion text, min_offset text, max_offset text, processed boolean, primary key ((jobName, topicName))
    //Partition=0 => [7793 -> 7862]
    //Partition=1 => [7733 -> 7842]
    //max_offset [7862, 7842]
    //min_offset [7862, 7842]

    //insert... where max_offset=currentMin and job_id=uuid
    //Traitement
    //data.rdd
    //update set processed=true
  }
}

class CustomSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    CustomSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "customSink"
}


object ExactlyOnceStructuredTest extends App {

  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("testStructured")
    .config("spark.task.maxFailures", "4")
    .config("spark.executor.memory", "1G")
    //.config("spark.hadoop.fs.ckfs.impl", "exactlyonce.CassandraSimpleFileSystem")
    //.config("spark.hadoop.cassandra.host", "10.240.0.114")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .getOrCreate()

  LogManager.getRootLogger().setLevel(Level.WARN)
  sparkSession.sparkContext.parallelize(Seq("")).foreachPartition(p => {
    LogManager.getRootLogger().setLevel(Level.WARN)
  })


  case class Person(name: String, age: Int) {
    def this() = this(null, 0)
  }


//  val root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).asInstanceOf[Nothing]
//  root.setLevel(Level.INFO)
//
  LogManager.getRootLogger.setLevel(Level.WARN)
  case class KafkaMessage(key: String, value: String, topic: String, partition: Int, offset: Long, timestamp: Long, timestampType: Int)

  object kafka {
    val producer = {
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      import org.apache.kafka.clients.producer.KafkaProducer
      new KafkaProducer[Nothing, String](props)
    }
  }


  import sparkSession.implicits._


  val schema = StructType(Seq(
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))

  import org.apache.spark.sql.functions.from_json

  val ds = sparkSession.readStream
    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("kafka.isolation.level", "read_committed")
    .option("startingOffsets", """{"exactlyonce":{"0":5000}}""")
    .option("failOnDataLoss", "true")
    .option("subscribe", "exactlyonce")
    .option("maxOffsetsPerTrigger", "100")
    .load()
    .withColumn("value", $"value".cast(StringType))
//    .select(from_json($"value", schema).as("json")).select("json.*").as[Person]
  //    .selectExpr("from_json(cast(value as string), 'age Integer, name String') as json").select("json.*").as[Person]


  import scala.concurrent.duration._

  //private val checkpointLocation = if(System.getenv("checkpointLocation") == null) "file:///home/quentin/Downloads/checkpoint1" else System.getenv("checkpointLocation")
  private val checkpointLocation = "file:///home/quentin/Downloads/checkpoint"+Random.nextInt()

  private val personWriter = new ForeachWriter[Person] {
    var i: Int = _

    override def open(partitionId: Long, version: Long) = true

    override def process(value: Person) = {
      println(s"VALUE=$value")
    }

    override def close(errorOrNull: Throwable) = {}
  }


  sparkSession.streams.addListener(new CustomStreamingQueryListener())

  val query = ds.writeStream
    //.trigger(Trigger.Once())
    .trigger(Trigger.ProcessingTime(1 seconds))
    .outputMode(OutputMode.Append())
    //.option("checkpointLocation", "/home/quentin/Downloads/checkpoint/1")
    .option("checkpointLocation", checkpointLocation)
    .format("exactlyonce.CustomSinkProvider")
    //.format("console")
    //.foreach(personWriter)

  val sq = query.start()
    //query.format("exactlyonce.CustomSinkProvider")
  LogManager.getRootLogger.setLevel(Level.INFO)

//  Thread.sleep(1000)
//
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Quentin", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Serge", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Raoul", "age": 20}"""))
//  println("sent")
//  Thread.sleep(5000)
//
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Quentin", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Serge", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Raoul", "age": 20}"""))
//  println("sent")
//  Thread.sleep(5000)
//
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Quentin", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Serge", "age": 20}"""))
//  kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Raoul", "age": 20}"""))
//  println("sent")
//
//  while (true){
//    kafka.producer.send(new ProducerRecord("test_structured", s"""{"name": "Raoul", "age": 20}"""))
//    Thread.sleep(500)
//  }
  sq.awaitTermination()


}


class CustomStreamingQueryListener extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"Started query with id : ${event.id}," +
      s" name: ${event.name},runId : ${event.runId}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress
    println(s"Streaming query made progress: ${progress.prettyJson}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Stream exited due to exception : ${event.exception},id : ${event.id}, " +
      s"runId: ${event.runId}")
  }

}