import java.net.URL

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.server.HiveServer2

object HiveServer {


  def main(args: Array[String]): Unit = {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

    val hiveConf = new HiveConf()
    val server = new HiveServer2

    server.init(hiveConf)
    server.start()

    server.getHiveConf.setAuxJars("ADD JAR file:///Users/constantine/Development/hdp/thesis-spatial/thesis-thrift-server/src/main/resources/jars/esri-geometry-api-1.2.1.jar")

//    val spark = SparkSession.builder
//      .appName("Spark Examples")
//      .config("spark.master", "local")
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sql("ADD JAR file:///Users/constantine/Development/hdp/thesis-spatial/thesis-thrift-server/src/main/resources/jars/esri-geometry-api-1.2.1.jar")


  }
}