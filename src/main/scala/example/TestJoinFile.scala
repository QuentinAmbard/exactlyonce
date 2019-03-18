package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
  * Created by quentin on 21/04/17.
  */
object TestJoinFile extends App {

  // configure the number of cores and RAM to use
  val conf = new SparkConf()
    .setMaster("local[5]") //Regler le paralallelisme ici
    .set("spark.cassandra.connection.host", "localhost")
    .setAppName("exactly-once")

  import com.datastax.spark.connector._


  val session = SparkSession.builder().config(conf).getOrCreate()
  //CREATE TABLE ent (id int primary key, name text);
  //Chargement de quelques data juste pour le test
  session.sparkContext.parallelize((1 to 20)).map(i => (i, "entreprise" + i)).saveToCassandra("exactlyonce", "ent")
  val schema = new StructType()
    .add("ent_id", IntegerType, true)
    .add("ent_other", IntegerType, true)


  val end_ids = session.read.format("csv")
    .option("header", "false")
    .schema(schema)
    .load("./id.csv")
    .createOrReplaceTempView("ent_id")
    //Pour voir ce qu'on a dans le fichier:
    //.show()

  //On monte la table en table temp, ce n'est pas necessaire dans le context DSE mais on doit le faire pour du local:
  session.read.format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> "exactlyonce", "table" -> "ent"))
    .load
    .createOrReplaceTempView("ent")

//  //Maintenant on peut faire n'importe quelle requete sql sur ces 2 tables:
//  session.sql("select ent.* from ent inner join ent_id where ent.id = ent_id.ent_id")
//    //show pour voir ce qu'il y a dedans
//    //.show()
//    .toLocalIterator().forEachRemaining(row => {
//    println(s"${row.getAs[Int]("ent_id")}, ${row.getAs[String]("name")}")
//  })

  //Et si c'est un champ text avec du json on peut appeler les methodes json de spark et ensuite explorer
  //le json avec ent.myJson.myfield dans la requête SQL ...
}
