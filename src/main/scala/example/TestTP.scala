package example

import org.apache.spark.SparkConf

/**
  * Created by quentin on 17/10/17.
  */
object TestTP extends App{
  val conf = new SparkConf()
    .setMaster("local[5]")
    .set("spark.task.maxFailures", "4")
    .set("spark.executor.memory", "1G")
    .setAppName("exactly-once")

}
