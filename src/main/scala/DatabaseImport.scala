import org.apache.spark.sql.SparkSession
import com.esri.core.geometry.ogc.OGCGeometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader
import org.apache.spark.sql.functions.udf

object DatabaseImport {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Examples")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val opts = Map(
      "url" -> "jdbc:postgresql://83.212.119.169:5430/",
      "driver" -> "org.postgresql.Driver",
      "user" -> "postgres",
      "password" -> "mysecretpassword",
      "dbtable" -> "tmp_geo_values")

    val df = spark
      .read
      .format("jdbc")
      .options(opts)
      .load

    import spark.implicits._

    val wkt2geoJSON = (wkbString: String) => {
      val aux: Array[Byte] = WKBReader.hexToBytes(wkbString)
      val geom: Geometry = new WKBReader().read(aux)
      val g0: OGCGeometry = OGCGeometry.fromText(geom.toString)
      g0.asGeoJson()
    }

    val wkt2geoJSONUDF = udf(wkt2geoJSON)
    df.withColumn("strdfgeo", wkt2geoJSONUDF('strdfgeo)).write.mode("overwrite").parquet("hdfs:///geo_values_parquet")

//    CREATE TABLE geo_values_parquet (id int, srid int, strdfgeo string)
//    ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'
//    STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
//    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
//
//    CREATE EXTERNAL TABLE geo_values (id int, srid int, strdfgeo  string) STORED AS PARQUET LOCATION 'hdfs:///geo_values_parquet';
  }
}
