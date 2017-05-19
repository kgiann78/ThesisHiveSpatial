import java.net.URL

import com.esri.core.geometry.ogc.OGCGeometry
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKBReader

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.functions.udf

case class Record(key: Int, value: String)

object ThesisServer {
  def main(args: Array[String]): Unit = {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

    var selection = ""
    if (args.length > 0) selection = args(0)

    val spark = selection match {
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

    //TODO http://stackoverflow.com/questions/31341498/save-spark-dataframe-as-dynamic-partitioned-table-in-hive

    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    //    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    //    df.createOrReplaceTempView("records")
    //    df.write.mode("overwrite").saveAsTable("records")
    //    df.write.partitionBy() //TODO new partitioner
    //
    //    val result = spark.sql("SELECT * FROM records")
    //    result.collect().foreach(println)

    HiveThriftServer2.startWithContext(spark.sqlContext)

    spark.sql("ADD JAR hdfs:///jars/esri-geometry-api-1.2.1.jar")
    spark.sql("ADD JAR hdfs:///jars/spatial-sdk-hive-1.2.1-SNAPSHOT.jar")
    spark.sql("ADD JAR hdfs:///jars/spatial-sdk-json-1.2.1-SNAPSHOT.jar")

    spark.sql("drop function if exists ST_AsBinary")
    spark.sql("drop function if exists ST_AsGeoJSON")
    spark.sql("drop function if exists ST_AsJSON")
    spark.sql("drop function if exists ST_AsShape")
    spark.sql("drop function if exists ST_AsText")
    spark.sql("drop function if exists ST_GeomFromJSON")
    spark.sql("drop function if exists ST_GeomFromGeoJSON")
    spark.sql("drop function if exists ST_GeomFromShape")
    spark.sql("drop function if exists ST_GeomFromText")
    spark.sql("drop function if exists ST_GeomFromWKB")
    spark.sql("drop function if exists ST_PointFromWKB")
    spark.sql("drop function if exists ST_LineFromWKB")
    spark.sql("drop function if exists ST_PolyFromWKB")
    spark.sql("drop function if exists ST_MPointFromWKB")
    spark.sql("drop function if exists ST_MLineFromWKB")
    spark.sql("drop function if exists ST_MPolyFromWKB")
    spark.sql("drop function if exists ST_GeomCollection")
    spark.sql("drop function if exists ST_GeometryType")
    spark.sql("drop function if exists ST_Point")
    spark.sql("drop function if exists ST_PointZ")
    spark.sql("drop function if exists ST_LineString")
    spark.sql("drop function if exists ST_Polygon")
    spark.sql("drop function if exists ST_MultiPoint")
    spark.sql("drop function if exists ST_MultiLineString")
    spark.sql("drop function if exists ST_MultiPolygon")
    spark.sql("drop function if exists ST_SetSRID")
    spark.sql("drop function if exists ST_SRID")
    spark.sql("drop function if exists ST_IsEmpty")
    spark.sql("drop function if exists ST_IsSimple")
    spark.sql("drop function if exists ST_Dimension")
    spark.sql("drop function if exists ST_X")
    spark.sql("drop function if exists ST_Y")
    spark.sql("drop function if exists ST_MinX")
    spark.sql("drop function if exists ST_MaxX")
    spark.sql("drop function if exists ST_MinY")
    spark.sql("drop function if exists ST_MaxY")
    spark.sql("drop function if exists ST_IsClosed")
    spark.sql("drop function if exists ST_IsRing")
    spark.sql("drop function if exists ST_Length")
    spark.sql("drop function if exists ST_GeodesicLengthWGS84")
    spark.sql("drop function if exists ST_Area")
    spark.sql("drop function if exists ST_Is3D")
    spark.sql("drop function if exists ST_Z")
    spark.sql("drop function if exists ST_MinZ")
    spark.sql("drop function if exists ST_MaxZ")
    spark.sql("drop function if exists ST_IsMeasured")
    spark.sql("drop function if exists ST_M")
    spark.sql("drop function if exists ST_MinM")
    spark.sql("drop function if exists ST_MaxM")
    spark.sql("drop function if exists ST_CoordDim")
    spark.sql("drop function if exists ST_NumPoints")
    spark.sql("drop function if exists ST_PointN")
    spark.sql("drop function if exists ST_StartPoint")
    spark.sql("drop function if exists ST_EndPoint")
    spark.sql("drop function if exists ST_ExteriorRing")
    spark.sql("drop function if exists ST_NumInteriorRing")
    spark.sql("drop function if exists ST_InteriorRingN")
    spark.sql("drop function if exists ST_NumGeometries")
    spark.sql("drop function if exists ST_GeometryN")
    spark.sql("drop function if exists ST_Centroid")
    spark.sql("drop function if exists ST_Contains")
    spark.sql("drop function if exists ST_Crosses")
    spark.sql("drop function if exists ST_Disjoint")
    spark.sql("drop function if exists ST_EnvIntersects")
    spark.sql("drop function if exists ST_Envelope")
    spark.sql("drop function if exists ST_Equals")
    spark.sql("drop function if exists ST_Overlaps")
    spark.sql("drop function if exists ST_Intersects")
    spark.sql("drop function if exists ST_Relate")
    spark.sql("drop function if exists ST_Touches")
    spark.sql("drop function if exists ST_Distance")
    spark.sql("drop function if exists ST_Boundary")
    spark.sql("drop function if exists ST_Buffer")
    spark.sql("drop function if exists ST_ConvexHull")
    spark.sql("drop function if exists ST_Intersection")
    spark.sql("drop function if exists ST_Union")
    spark.sql("drop function if exists ST_Difference")
    spark.sql("drop function if exists ST_SymmetricDiff")
    spark.sql("drop function if exists ST_SymDifference")
    spark.sql("drop function if exists ST_Aggr_ConvexHull")
    spark.sql("drop function if exists ST_Aggr_Intersection")
    spark.sql("drop function if exists ST_Aggr_Union")
    spark.sql("drop function if exists ST_Bin")
    spark.sql("drop function if exists ST_BinEnvelope")

    spark.sql("create function ST_AsBinary as 'com.esri.hadoop.hive.ST_AsBinary'")
    spark.sql("create function ST_AsGeoJSON as 'com.esri.hadoop.hive.ST_AsGeoJson'")
    spark.sql("create function ST_AsJSON as 'com.esri.hadoop.hive.ST_AsJson'")
    spark.sql("create function ST_AsShape as 'com.esri.hadoop.hive.ST_AsShape'")
    spark.sql("create function ST_AsText as 'com.esri.hadoop.hive.ST_AsText'")
    spark.sql("create function ST_GeomFromJSON as 'com.esri.hadoop.hive.ST_GeomFromJson'")
    spark.sql("create function ST_GeomFromGeoJSON as 'com.esri.hadoop.hive.ST_GeomFromGeoJson'")
    spark.sql("create function ST_GeomFromShape as 'com.esri.hadoop.hive.ST_GeomFromShape'")
    spark.sql("create function ST_GeomFromText as 'com.esri.hadoop.hive.ST_GeomFromText'")
    spark.sql("create function ST_GeomFromWKB as 'com.esri.hadoop.hive.ST_GeomFromWKB'")
    spark.sql("create function ST_PointFromWKB as 'com.esri.hadoop.hive.ST_PointFromWKB'")
    spark.sql("create function ST_LineFromWKB as 'com.esri.hadoop.hive.ST_LineFromWKB'")
    spark.sql("create function ST_PolyFromWKB as 'com.esri.hadoop.hive.ST_PolyFromWKB'")
    spark.sql("create function ST_MPointFromWKB as 'com.esri.hadoop.hive.ST_MPointFromWKB'")
    spark.sql("create function ST_MLineFromWKB as 'com.esri.hadoop.hive.ST_MLineFromWKB'")
    spark.sql("create function ST_MPolyFromWKB as 'com.esri.hadoop.hive.ST_MPolyFromWKB'")
    spark.sql("create function ST_GeomCollection as 'com.esri.hadoop.hive.ST_GeomCollection'")
    spark.sql("create function ST_GeometryType as 'com.esri.hadoop.hive.ST_GeometryType'")
    spark.sql("create function ST_Point as 'com.esri.hadoop.hive.ST_Point'")
    spark.sql("create function ST_PointZ as 'com.esri.hadoop.hive.ST_PointZ'")
    spark.sql("create function ST_LineString as 'com.esri.hadoop.hive.ST_LineString'")
    spark.sql("create function ST_Polygon as 'com.esri.hadoop.hive.ST_Polygon'")
    spark.sql("create function ST_MultiPoint as 'com.esri.hadoop.hive.ST_MultiPoint'")
    spark.sql("create function ST_MultiLineString as 'com.esri.hadoop.hive.ST_MultiLineString'")
    spark.sql("create function ST_MultiPolygon as 'com.esri.hadoop.hive.ST_MultiPolygon'")
    spark.sql("create function ST_SetSRID as 'com.esri.hadoop.hive.ST_SetSRID'")
    spark.sql("create function ST_SRID as 'com.esri.hadoop.hive.ST_SRID'")
    spark.sql("create function ST_IsEmpty as 'com.esri.hadoop.hive.ST_IsEmpty'")
    spark.sql("create function ST_IsSimple as 'com.esri.hadoop.hive.ST_IsSimple'")
    spark.sql("create function ST_Dimension as 'com.esri.hadoop.hive.ST_Dimension'")
    spark.sql("create function ST_X as 'com.esri.hadoop.hive.ST_X'")
    spark.sql("create function ST_Y as 'com.esri.hadoop.hive.ST_Y'")
    spark.sql("create function ST_MinX as 'com.esri.hadoop.hive.ST_MinX'")
    spark.sql("create function ST_MaxX as 'com.esri.hadoop.hive.ST_MaxX'")
    spark.sql("create function ST_MinY as 'com.esri.hadoop.hive.ST_MinY'")
    spark.sql("create function ST_MaxY as 'com.esri.hadoop.hive.ST_MaxY'")
    spark.sql("create function ST_IsClosed as 'com.esri.hadoop.hive.ST_IsClosed'")
    spark.sql("create function ST_IsRing as 'com.esri.hadoop.hive.ST_IsRing'")
    spark.sql("create function ST_Length as 'com.esri.hadoop.hive.ST_Length'")
    spark.sql("create function ST_GeodesicLengthWGS84 as 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84'")
    spark.sql("create function ST_Area as 'com.esri.hadoop.hive.ST_Area'")
    spark.sql("create function ST_Is3D as 'com.esri.hadoop.hive.ST_Is3D'")
    spark.sql("create function ST_Z as 'com.esri.hadoop.hive.ST_Z'")
    spark.sql("create function ST_MinZ as 'com.esri.hadoop.hive.ST_MinZ'")
    spark.sql("create function ST_MaxZ as 'com.esri.hadoop.hive.ST_MaxZ'")
    spark.sql("create function ST_IsMeasured as 'com.esri.hadoop.hive.ST_IsMeasured'")
    spark.sql("create function ST_M as 'com.esri.hadoop.hive.ST_M'")
    spark.sql("create function ST_MinM as 'com.esri.hadoop.hive.ST_MinM'")
    spark.sql("create function ST_MaxM as 'com.esri.hadoop.hive.ST_MaxM'")
    spark.sql("create function ST_CoordDim as 'com.esri.hadoop.hive.ST_CoordDim'")
    spark.sql("create function ST_NumPoints as 'com.esri.hadoop.hive.ST_NumPoints'")
    spark.sql("create function ST_PointN as 'com.esri.hadoop.hive.ST_PointN'")
    spark.sql("create function ST_StartPoint as 'com.esri.hadoop.hive.ST_StartPoint'")
    spark.sql("create function ST_EndPoint as 'com.esri.hadoop.hive.ST_EndPoint'")
    spark.sql("create function ST_ExteriorRing as 'com.esri.hadoop.hive.ST_ExteriorRing'")
    spark.sql("create function ST_NumInteriorRing as 'com.esri.hadoop.hive.ST_NumInteriorRing'")
    spark.sql("create function ST_InteriorRingN as 'com.esri.hadoop.hive.ST_InteriorRingN'")
    spark.sql("create function ST_NumGeometries as 'com.esri.hadoop.hive.ST_NumGeometries'")
    spark.sql("create function ST_GeometryN as 'com.esri.hadoop.hive.ST_GeometryN'")
    spark.sql("create function ST_Centroid as 'com.esri.hadoop.hive.ST_Centroid'")
    spark.sql("create function ST_Contains as 'com.esri.hadoop.hive.ST_Contains'")
    spark.sql("create function ST_Crosses as 'com.esri.hadoop.hive.ST_Crosses'")
    spark.sql("create function ST_Disjoint as 'com.esri.hadoop.hive.ST_Disjoint'")
    spark.sql("create function ST_EnvIntersects as 'com.esri.hadoop.hive.ST_EnvIntersects'")
    spark.sql("create function ST_Envelope as 'com.esri.hadoop.hive.ST_Envelope'")
    spark.sql("create function ST_Equals as 'com.esri.hadoop.hive.ST_Equals'")
    spark.sql("create function ST_Overlaps as 'com.esri.hadoop.hive.ST_Overlaps'")
    spark.sql("create function ST_Intersects as 'com.esri.hadoop.hive.ST_Intersects'")
    spark.sql("create function ST_Relate as 'com.esri.hadoop.hive.ST_Relate'")
    spark.sql("create function ST_Touches as 'com.esri.hadoop.hive.ST_Touches'")
    spark.sql("create function ST_Distance as 'com.esri.hadoop.hive.ST_Distance'")
    spark.sql("create function ST_Boundary as 'com.esri.hadoop.hive.ST_Boundary'")
    spark.sql("create function ST_Buffer as 'com.esri.hadoop.hive.ST_Buffer'")
    spark.sql("create function ST_ConvexHull as 'com.esri.hadoop.hive.ST_ConvexHull'")
    spark.sql("create function ST_Intersection as 'com.esri.hadoop.hive.ST_Intersection'")
    spark.sql("create function ST_Union as 'com.esri.hadoop.hive.ST_Union'")
    spark.sql("create function ST_Difference as 'com.esri.hadoop.hive.ST_Difference'")
    spark.sql("create function ST_SymmetricDiff as 'com.esri.hadoop.hive.ST_SymmetricDiff'")
    spark.sql("create function ST_SymDifference as 'com.esri.hadoop.hive.ST_SymmetricDiff'")
    spark.sql("create function ST_Aggr_ConvexHull as 'com.esri.hadoop.hive.ST_Aggr_ConvexHull'")
    spark.sql("create function ST_Aggr_Intersection as 'com.esri.hadoop.hive.ST_Aggr_Intersection'")
    spark.sql("create function ST_Aggr_Union as 'com.esri.hadoop.hive.ST_Aggr_Union'")
    spark.sql("create function ST_Bin as 'com.esri.hadoop.hive.ST_Bin'")
    spark.sql("create function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope'")

    val opts = Map(
      "url" -> "jdbc:postgresql://83.212.119.169:5430/",
      "driver" -> "org.postgresql.Driver",
      "user" -> "postgres",
      "password" -> "mysecretpassword",
      "dbtable" -> "temp_geo_values")

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
    df.withColumn("json", wkt2geoJSONUDF('strdfgeo)).write.mode("overwrite").parquet("hdfs:///geo_values")


    //spark.sql("CREATE TABLE demo_shape_point(shape string) STORED AS ORC")

    //spark.sql("INSERT INTO demo_shape_point VALUES ('POINT (-74.140007019999985 39.650001530000054)')")

    //spark.sql("SELECT * FROM demo_shape_point").collect().foreach(println)

    //spark.sql("SELECT ST_AsJson(ST_Polygon(1.5,2.5, 3.0,2.2, 2.2,1.1))").collect().foreach(println)

    //spark.close()
  }
}
