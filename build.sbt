import sbtassembly.PathList

//sbt clean assembly
//dse spark-submit --class exactlyonce.ExactlyOnceTestForEachKill /home/quentin/projects/spark-scala-kafka/target/scala-2.11/spark-scala-kafka-assembly-0.1.jar
//dse spark-submit --class exactlyonce.TestSimpleStream /home/quentin/projects/spark-scala-kafka/target/scala-2.11/spark-scala-kafka-assembly-0.1.jar
//dse spark-submit --deploy-mode cluster --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=cassandra  --class exactlyonce.TestSimpleStream spark-scala-kafka-assembly-0.1.jar
version := "0.1"

scalaVersion := "2.11.1"
//scalaVersion := "2.10.7"

//val sparkVersion = "2.2.0"
//val sparkVersion = "2.3.1"
val sparkVersion = "2.2.3"
//val sparkVersion = "2.0.2"
val sparkVersion_snapshot = "2.4.0-SNAPSHOT"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

//val scope = "provided"
val scope = "compile"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += "confluent" at "http://packages.confluent.io/maven/"
resolvers += "confluents" at "https://packages.confluent.io/maven/"


libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.0"  % scope,

//"net.liftweb" %% "lift-json" % "2.6.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.8"  % scope,
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0",
  //"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.4.5",
  //"com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.6.6",

//  "org.apache.spark" %% "spark-core"      % sparkVersion,
//  "org.apache.spark" %% "spark-sql"       % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  "org.apache.spark" %% "spark-core"             % sparkVersion % scope,
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % scope,
  "org.apache.spark"  %% "spark-streaming"       % sparkVersion % scope,

  //"org.apache.kafka" %% "kafka" % "0.11.0.0" % "provided"

  //"org.apache.spark"  %% "spark-streaming-kafka-0-8" % sparkVersion
  "org.apache.spark"  %% "spark-streaming-kafka-0-10" % sparkVersion, //sparkVersion_snapshot, sparkVersion
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "1.1.1",
  "io.confluent" % "kafka-avro-serializer" % "5.0.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "org.coursera" % "dropwizard-metrics-datadog" % "1.1.13"

)



// resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
//  case x =>
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
}

