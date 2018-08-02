################ PySpark shell ##################
# Start the spark shell with the require lib. Can also use --jars with the list of local jars.
# dse pyspark --packages com.datastax.spark:spark-cassandra-connector_2.10:1.6.0,com.databricks:spark-csv_2.10:1.5.0  --conf spark.cassandra.connection.host=127.0.0.1

################ Spark submit ##################
# dse spark-submit --master spark://127.0.0.1:7077 --num-executors 1 --executor-memory 1G --executor-cores 2 --driver-memory 1G pysparkExample.py

################# Quickly start a driver on your machine without spark submit. Doesn't let you add external jar easily #################
############################ (cassandra-spark-connector can be added as default jar on each worker though) #############################
# export SPARK_HOME=/home/quentin/tools/spark/spark-1.6.3-bin-hadoop2.6/
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
# export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

from pyspark import SparkContext, SparkConf, SQLContext, HiveContext

conf = SparkConf().setAppName('appName').setMaster('local').set("spark.cassandra.connection.host", "127.0.0.1") #spark://xxx.xx.xx.xx:7077
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

################# inside pyspark shell ################
# CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
# CREATE TABLE test.mytable (key int primary key, v1 int, v2 int);
###Write som sample to the table
sc.parallelize(range(100000)).map(lambda i: (i, i,i)).toDF(['key', 'v1', 'v2']).write.format("org.apache.spark.sql.cassandra").mode('append').options(table="mytable", keyspace="test").save()

###register a tempTable
testDF = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="mytable", keyspace="test").load()
# Might want to cache the result to avoid reading from C* again and again (saved in memory + disk if too many data)
testDF.cache()
testDF.createOrReplaceTempView("test")
testDF.show()
# Whatever sql query you need here. Keep in mind that if you don't specify any partition key, nothing will be pushed down to C* (full table scan)
result = sqlContext.sql("SELECT * FROM test WHERE v1 > 200 ").cache()
result.show()
result.count()

#####Save the result in a csv file but distributed accross all machine
result.write.format('com.databricks.spark.csv').save('/home/quentin/Download/mycsv.csv')

#####If you have a small dataset, you can collect everything to your driver (might need to increase the driver memory)
local_result = result.collect()
# write
import csv
with open('/home/quentin/Download/mycsv2.csv', "wb") as file:
    writer = csv.writer(file)
    for row in local_result:
        writer.writerow(row)
