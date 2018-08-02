package avro

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object AvroProducerTest extends App {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
  props.put("schema.registry.url", "http://localhost:8081")
  val producer = new KafkaProducer[String, User](props)

  //http://localhost:8081/subjects/user-key/versions/
  //http://localhost:8081/subjects/user-key/versions/
  //http://localhost:8081/subjects/user-value/versions/1
  //curl -X DELETE http://localhost:8081/subjects/user-value

//  curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
//  --data '{"schema": "{\"namespace\": \"avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [     {\"name\": \"firstname\", \"type\": \"string\"},     {\"name\": \"lastname\",  \"type\": \"string\"} ]}"}' \
//  http://localhost:8081/subjects/user-value/versions

  (1 to 30).map(i =>{
    new ProducerRecord("user", "key"+i, new User("firstname"+i, "lastname"+i))
  }).foreach(producer.send(_))

//  val userSchema = """{"type":"record","name":"user","fields":[{"name":"firstname","type":"string"}, {"name":"lastname","type":"string"}]}"""
//  val parser = new Schema.Parser()
//  val schema = parser.parse(userSchema)
//  (1 to 30).map(i =>{
//    val avroRecord = new GenericData.Record(schema)
//    avroRecord.put("firstname", "first"+i)
//    avroRecord.put("lastname", "last"+i)
//    new ProducerRecord[String, Object]("user", "key"+i, avroRecord)
//  }).foreach(producer.send(_))

  producer.flush()
  producer.close()

}

