package example

import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by quentin on 21/04/17.
  */
object TestJoinWithCassandra extends App {

  // configure the number of cores and RAM to use
  val conf = new SparkConf()
    .setMaster("local[5]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.executor.memory", "1G")
    .setAppName("exactly-once")



  /**
    * Read from sparksql
    */
  val session = SparkSession.builder().config(conf).getOrCreate()
//  session
//    .read
//    .format("org.apache.spark.sql.cassandra")
//    .options(Map("keyspace" -> "exactlyonce", "table" -> "test2"))
//    .load
//    .createOrReplaceTempView("test2")
//  session.sql("SELECT map['test'] FROM test2").show()

  //spark.read.json(Seq("aze").toDS()).createOrReplaceTempView("direct_raw_baby_name")

  /**
    * WRITE TIMEUUID TO CASSANDRA WITH DATAFRAME
    */
//    import org.apache.spark.sql.cassandra._
//    import org.apache.spark.sql.functions._
//    import session.implicits._
//    val df = session.sparkContext.parallelize((1 to 20)).map(i => (i, "day" + i, "bucket" + i, "someText")).toDF("partitioner", "day", "bucket", "val")
//    val timeuuidUdf = udf[String](UUIDs.timeBased().toString)
//    df.withColumn("time", timeuuidUdf()).write
//      .cassandraFormat("test", "exactlyonce")
//      .mode(SaveMode.Append)
//      .save()

  /**
    * READ FROM MULTIPLE PARTITION WITH RDD AND JOIN (instead of a sparksql IN(...))
    */

  import com.datastax.spark.connector._
  import session.implicits._

  session.sparkContext.parallelize((1 to 20)).map(i => (i, "d" + i, "bucket" + i)).toDF()
//  create table exactlyonce.test (id int primary key, day text, value int);
//  insert into exactlyonce.test (id, day, value) values (1, 'd1', 1);
//  insert into exactlyonce.test (id, day, value) values (2, 'd2', 2);
//  insert into exactlyonce.test (id, day, value) values (3, 'd3', 3);
val partitionKeys = session.sparkContext.parallelize((1 to 20)).map(i => (i, "d" + i, "bucket" + i))
  partitionKeys.repartitionByCassandraReplica("exactlyonce", "test").joinWithCassandraTable("exactlyonce", "test", SomeColumns("id", "day")).foreach(println(_))


  case class PK(partitioner: Int, day: String, bucket: String)
  val partitionKeysClass = session.sparkContext.parallelize((1 to 24)).map(i => PK(i, "day1", "bucket" + i))
  partitionKeysClass.repartitionByCassandraReplica("exactlyonce", "test").joinWithCassandraTable("exactlyonce", "test").foreach(println(_))

  case class PK2(dummy: Int, day: String, bucket: String, toto: String = "42")
  val partitionKeysClass2 = session.sparkContext.parallelize((1 to 20)).map(i => PK2(i, "day" + i, "bucket" + i))
  partitionKeysClass2.repartitionByCassandraReplica("exactlyonce", "test", partitionKeyMapper = SomeColumns("partitioner" as "dummy", "day" as "day", "bucket" as "bucket"))
    .joinWithCassandraTable("exactlyonce", "test", joinColumns = SomeColumns("partitioner" as "dummy", "day" as "day", "bucket" as "bucket"))
    .foreach(println(_))

}
