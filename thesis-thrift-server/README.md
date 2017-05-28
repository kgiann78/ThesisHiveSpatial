
Prior to running the Thrift-Server:

Withing the hadoop-master (as of here https://github.com/kgiann78/hadoop-cluster-spark-docker)
root directory add the following jars to hive:

1. Copy jars directory to root directory of hdfs:

        hadoop fs -put /root/thesis-spatial/thesis-thrift-server/src/main/resources/jars/ /
2. Grant ownership on these jar files to hive:hdfs

        hadoop fs -chown -R hive:hdfs /jars

Then run the ThesisServer application with spark-submit. This application
 will copy the jars from hive to the spark instance and it will also create
 all necessary ESRI Geometry functions.

    spark-submit \
    --class ThesisServer \
    --master yarn \
    --deploy-mode client \
    /root/thesis-spatial/thesis-thrift-server/target/hive-thrift-server-jar-with-dependencies.jar

After the thrift-server is started from a terminal open a beeline connection as follows:

    !connect jdbc:hive2://host_name_or_host_ip:10000 username password