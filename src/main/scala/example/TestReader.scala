package example

import exactlyonce.TestSimpleStream.ssc
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.cassandra.CassandraSQLRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by quentin on 21/04/17.
  */
object TestReader extends App {

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

  import com.datastax.spark.connector._
//  import session.implicits._
//
//
////  val schema: StructType = new StructType()
////    .add("a", "string")
////    .add("b", "string")
//  import org.apache.spark.sql.cassandra.CassandraSQLRow._
//  import org.apache.spark.sql.cassandra._

//    val myRDD: RDD[Row] = session.sparkContext.parallelize(Seq("A", "B")).joinWithCassandraTable[CassandraSQLRow]("ks", "name").values.map(r => new CassandraSQLRow(r.metaData, r.columnValues.map{ v => v match {
//      case UTF8String => v.toString
//      case _ => r
//    }}) ).asInstanceOf[RDD[Row]]
//  val schema = session.read.cassandraFormat("ks", "name").load().schema
//  session.sqlContext.createDataFrame(myRDD, schema)


//  session.sparkContext.parallelize((1 to 20)).map(i => (i, "d" + i, "bucket" + i)).toDF()
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
