package exactlyonce

import java.util.UUID

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types.StringType


case class CustomSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import data.sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //Au début du batch, récupérer les offests sur la partition
    val offsets = data.groupBy($"partition").agg(min($"offset").as("min_offset"), max($"offset").as("max_offset")).select("partition", "min_offset", "max_offset").collect()
    offsets.foreach(row => {
      println(s"Partition=${row.getAs[Int]("partition")} => [${row.getAs[Int]("min_offset")} -> ${row.getAs[Int]("max_offset")}]")
    })
    //Ensuite utiliser exactement le meme process de checkpoint que celui mis en place, ca permettra de redémarrer avec la meme plage d'offset
    //create table offsets (jobName text, topicName text, min_offset text, max_offset text, processed boolean, primary key ((jobName, topicName))
    //Partition=0 => [7793 -> 7862]
    //Partition=1 => [7733 -> 7842]
    //max_offset [7862, 7842]
    //min_offset [7862, 7842]

    //insert into offsets (jobName, topicName) values ('test-exactlyonce', 'exactlyonce',  where max_offset=currentMin and job_id=uuid
    //Traitement
    //MyClass.processSTETLogique(data)
    //update casandra set processed=true
  }
}

class CustomSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    CustomSink(sqlContext, parameters, partitionColumns, outputMode)
  }

  override def shortName(): String = "customSink"
}


object ExactlyOnceStructuredTestStet extends App {

  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("testStructured")
    .config("spark.task.maxFailures", "4")
    .config("spark.executor.memory", "1G")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .getOrCreate()

  LogManager.getRootLogger().setLevel(Level.WARN)

  import sparkSession.implicits._

  //TODO: au démarrage, faire un select dans C*, récupérer le dernier batch et le rejouer avec un batch spark/kafka s'il n'est pas terminé
  //Ca permet de rejouer exactement avec les memes offsets
  //  val df = spark
  //    .read
  //    .format("kafka")
  //    .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  //    .option("subscribe", "topic1,topic2")
  //    .option("startingOffsets", """{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}""")
  //    .option("endingOffsets", """{"topic1":{"0":50,"1":-1},"topic2":{"0":-1}}""")
  //    .load()
  //MyClass.processSTETLogique(df)
  //TODO mettre a jour les offsets de fin kafka


  val ds = sparkSession.readStream
    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("kafka.isolation.level", "read_committed")
    //Attention, ca ne marche que si le repertoire de checkpoint est vide.
    //TODO: a recupérer de la table cassandra et utiliser les offsets de fin
    // (un batch incomplet sera rejoué par le traitement du dessous avant de démarrer le stream)
    .option("startingOffsets", """{"exactlyonce":{"0":5000}}""")
    .option("failOnDataLoss", "true")
    .option("subscribe", "exactlyonce")
    .option("maxOffsetsPerTrigger", "100")
    .load()
    .withColumn("value", $"value".cast(StringType))

  import scala.concurrent.duration._

  //A chaque redémarrage, on repart d'un nouveau checkpoint.
  //Ca permet de pouvoir forcer les offsets de démarrage a la main
  private val checkpointLocation = "file:///home/quentin/Downloads/checkpoint" + UUID.randomUUID()

  sparkSession.streams.addListener(new CustomStreamingQueryListener())

  val query = ds.writeStream
    //.trigger(Trigger.Once())
    .trigger(Trigger.ProcessingTime(1 seconds))
    .outputMode(OutputMode.Append())
    .option("checkpointLocation", checkpointLocation)
    .format("exactlyonce.CustomSinkProvider")

  val sq = query.start()
  sq.awaitTermination()


}

//Peut etre utile pour tracer les evenements mais tout est asynchrone donc on ne peut pas s'en servir pour sauvegarder les offsets
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