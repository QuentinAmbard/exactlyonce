package avro

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable

//TODO synchronized or avoid map?
object AvroDeserializer {

  val props = Map("schema.registry.url" -> "http://localhost:8081",     "auto.offset.reset" -> "earliest").asJava

  private var deserializers = mutable.Map[String, KafkaAvroDeserializer]()
  private var schemas = mutable.Map[String, Schema]()
  private var restService: RestService = null

  def initTopic(topic: String) = {
    val confluentSchema = getRestService.getLatestVersion(topic+"-value")
    val schema = new Schema.Parser().parse(confluentSchema.getSchema)
    schemas(topic) = schema
    val deserializer = new KafkaAvroDeserializer
    deserializer.configure(props, true)
    deserializers(topic) = deserializer
  }

  def getValueDeserializerAndSchema(topic: String) = {
    if(!schemas.contains(topic)){
      initTopic(topic)
    }
    (deserializers(topic), schemas(topic))
  }

  private def getRestService = {
    if (restService == null) {
      restService = new RestService("http://localhost:8081")
    }
    restService
  }

}
object AvroStructuredStreaming extends App {

  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("testStructured")
    .getOrCreate()

  LogManager.getRootLogger.setLevel(Level.WARN)

  import sparkSession.implicits._

  case class UserTest(firstname: String, lastname: String)
  val schema = StructType(Seq(
    StructField("firstname", StringType, true),
    StructField("lastname", StringType, true)
  ))


  import org.apache.spark.sql.functions.udf

  //TODO don't know why, using ReflectDatumReader this set all fields to ""
  //  val reader = new ReflectDatumReader[User](schema)
  //  val user = reader.read(null, DecoderFactory.get().binaryDecoder(value, null));
  val deserializeValue = udf((value: Array[Byte]) => {
    val (deserializer, schema) = AvroDeserializer.getValueDeserializerAndSchema("user")
    val userRecord  = deserializer.deserialize("user", value, schema).asInstanceOf[org.apache.avro.generic.GenericData.Record]
    UserTest(userRecord.get("firstname").toString, userRecord.get("lastname").toString)
  })

  val ds = sparkSession.readStream
    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")

    .option("subscribe", "user")
    .option("maxOffsetsPerTrigger", "100")
    .load()
    .withColumn("value", deserializeValue($"value"))
    .withColumn("firstname", $"value.firstname")
    .withColumn("lastname", $"value.lastname")
    .withColumn("key", $"key".cast(StringType))
    .select("offset","key","lastname", "firstname")

  private val checkpointLocation = "file:///home/quentin/Downloads/checkpoint"

  val query = ds.writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .option("checkpointLocation", checkpointLocation)
    .queryName("test")

  println("Starting streaming query")
  val sq = query.start()
  //LogManager.getRootLogger.setLevel(Level.INFO)

  sq.awaitTermination()

}
