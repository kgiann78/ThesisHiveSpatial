import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import org.apache.spark.sql.SparkSession
import com.esri.core.geometry.ogc.OGCGeometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._


object DatabaseImport {
  def main(args: Array[String]): Unit = {
    val parser = new CmdLineParser(CliArgs)

    try {
      parser.parseArgument(args.toList.asJava)
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }

    val spark = CliArgs.spark_mode match {
      case "cluster" => SparkSession.builder
        .appName("Spark Examples")
        .enableHiveSupport()
        .getOrCreate()
      case _ => SparkSession.builder
        .appName("Spark Examples")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    }

    import spark.implicits._
    val connectionString =  "jdbc:postgresql://" + CliArgs.host +":" + CliArgs.port + "/"

    val opts = Map(
      "url" -> connectionString,
//      "url" -> "jdbc:postgresql://83.212.119.169:5430/",
      "driver" -> "org.postgresql.Driver",
      "user" -> "postgres",
      "password" -> "mysecretpassword",
      "dbtable" -> CliArgs.table)

    val df = spark
      .read
      .format("jdbc")
      .options(opts)
      .load

    if (CliArgs.convert != null && !CliArgs.convert.isEmpty) {

      if (CliArgs.field == null
        || (CliArgs.field != null && CliArgs.field.isEmpty)
        || CliArgs.table == null
        || (CliArgs.table != null && CliArgs.table.isEmpty)) return

      CliArgs.convert match {
        case "wkt2geoJSON" =>
          val wkt2geoJSON = (wkbString: String) => {
            val aux: Array[Byte] = WKBReader.hexToBytes(wkbString)
            val geom: Geometry = new WKBReader().read(aux)
            val g0: OGCGeometry = OGCGeometry.fromText(geom.toString)
            g0.asGeoJson()
          }

          val wkt2geoJSONUDF = udf(wkt2geoJSON)

          df.withColumn(CliArgs.field, wkt2geoJSONUDF(df.col(CliArgs.field)))
            .write.mode("overwrite")
            .parquet("hdfs:///" + CliArgs.table + "_parquet")

        case "wkt2text" =>
          val wkt2text = (wkbString: String) => {
            val aux: Array[Byte] = WKBReader.hexToBytes(wkbString)
            val geom: Geometry = new WKBReader().read(aux)
            val g0: OGCGeometry = OGCGeometry.fromText(geom.toString)
            g0.asText()
          }

          spark.sqlContext.udf.register("wkt2text", wkt2text)

          val wkt2textUDF = udf(wkt2text)

          println("Creating table " + CliArgs.table + "_parquet to hive")
          df.withColumn(CliArgs.field, wkt2textUDF(df.col(CliArgs.field)))
            .write.mode("overwrite")
            .parquet("hdfs:///" + CliArgs.table + "_parquet")

        case _ =>
          println(CliArgs.convert + " is not yet implemented")
      }
    } else {
      println("Creating table " + CliArgs.table + "_parquet to hive")

      df.write.mode("overwrite")
        .parquet("hdfs:///" + CliArgs.table + "_parquet")
    }


    //    CREATE TABLE geo_values_parquet (id int, srid int, strdfgeo string)
    //    ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'
    //    STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
    //    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
    //
    //    CREATE EXTERNAL TABLE geo_values (id int, srid int, strdfgeo  string) STORED AS PARQUET LOCATION 'hdfs:///geo_values_parquet';

  }


  object CliArgs {

    @Option(name = "-host", required = true,
      usage = "The host address to the postgres database")
    var host: String = "83.212.119.169"

    @Option(name = "-port", required = true,
      usage = "The port of the postgres database")
    var port: String = "5430"

    @Option(name = "-mode", required = true,
      usage = "Declares spark session mode (client or cluster)")
    var spark_mode: String = _

    @Option(name = "-table", required = true,
      usage = "Table to prepare")
    var table: String = _

    @Option(name = "-convert",
      usage = "Conversion method to use (wkt2geoJSON or wkt2text) ")
    var convert: String = _

    @Option(name = "-field",
      usage = "Field to provide modification")
    var field: String = _
  }


  //spark.sql("CREATE TABLE demo_shape_point(shape string) STORED AS ORC")

  //spark.sql("INSERT INTO demo_shape_point VALUES ('POINT (-74.140007019999985 39.650001530000054)')")

  //spark.sql("SELECT * FROM demo_shape_point").collect().foreach(println)

  //spark.sql("SELECT ST_AsJson(ST_Polygon(1.5,2.5, 3.0,2.2, 2.2,1.1))").collect().foreach(println)

  //spark.close()

}
