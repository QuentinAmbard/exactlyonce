package example;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.datastax.spark.connector.*;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import scala.Option;
import scala.Option$;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq$;
import scala.collection.mutable.Seq;

import java.util.ArrayList;
import java.util.List;


public class TestJoinWithCassandraJava {

    public static void main(String[] args) {
        new TestJoinWithCassandraJava().test();
    }

    public void test () {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[4]");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<ColumnRef> columns = new ArrayList<>();
        columns.add(column("val").as("value"));
        scala.collection.Seq<ColumnRef> scalaColumns = JavaConverters.collectionAsScalaIterableConverter(columns).asScala().toSeq();

        JavaRDD<Tuple3<Integer, String, String>> partitionKeysJava = context.parallelize(Lists.newArrayList(new Tuple3<Integer, String, String>(1, "day1", "bucket1")));
        CassandraJavaPairRDD<Tuple3<Integer, String, String>, String> rddJoined = javaFunctions(partitionKeysJava).joinWithCassandraTable("exactlyonce", "test",
                SomeColumns$.MODULE$.apply(scalaColumns), someColumns("partitioner", "day", "bucket"),
                mapColumnTo(String.class), mapTupleToRow(Integer.class, String.class, String.class));

        rddJoined.foreach(tuple3StringTuple2 -> System.out.println(tuple3StringTuple2._2()));

        System.out.println("count="+rddJoined.count());
    }

}
