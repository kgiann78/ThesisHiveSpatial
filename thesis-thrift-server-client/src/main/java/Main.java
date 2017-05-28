import java.sql.*;

public class Main {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {

        String host = "snf-649502.vm.okeanos.grnet.gr";
        String filepath = "/Users/constantine/Development/IdeaProjects/SparkHiveClient/src/main/resources/data.txt";

        ThriftServerClient client = new ThriftServerClient(host, "constantine", "");
//        client.execute("show databases");
//        client.execute("CREATE EXTERNAL TABLE aswkt_235007 " +
//                "(ctx int, subj int, obj int, expl boolean) " +
//                "STORED AS PARQUET LOCATION " +
//                "'hdfs:///aswkt_235007_parquet'");
//        client.execute("DROP TABLE aswkt_235007");

//        client.execute("CREATE EXTERNAL TABLE type_180829 " +
//                "(ctx int, subj int, obj int, expl boolean) " +
//                "STORED AS PARQUET LOCATION " +
//                "'hdfs:///type_180829_parquet'");
//        client.execute("DROP TABLE type_180829");

//        client.execute("CREATE EXTERNAL TABLE type_855540 " +
//                "(ctx int, subj int, obj int, expl boolean) " +
//                " STORED AS PARQUET LOCATION " +
//                "'hdfs:///type_855540_parquet'");
//        client.execute("DROP TABLE type_855540");


//        client.execute("CREATE TABLE tmp_geo_values_parquet (id int, srid int, strdfgeo binary) \n" +
//                "ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde' \n" +
//                "STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'\n" +
//                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
////        client.execute("DROP TABLE tmp_geo_values_parquet");


//        client.execute("CREATE EXTERNAL TABLE tmp_geo_values " +
//                "(id int, srid int, strdfgeo  string) " +
//                " STORED AS PARQUET LOCATION " +
//                "'hdfs:///tmp_geo_values_parquet'");
//        client.execute("DROP TABLE tmp_geo_values");

        client.execute("show tables");

//        client.execute("SELECT count(*) FROM type_855540 WHERE obj='805450457'");
//        client.execute("SELECT COUNT(*) FROM tmp_geo_values l_o2 " +
//                "JOIN tmp_geo_values l_o1 ON " +
//                "ST_Intersects(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(l_o1.strdfgeo))),ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(l_o2.strdfgeo))))");

//        client.execute("create temporary function ST_GeomFromWKT as 'com.esri.hadoop.hive.ST_GeomFromWKT'");
//        client.execute("SELECT ST_GeomFromWKT(strdfgeo) FROM tmp_geo_values limit 10");


//        l_o2\n" +
//                " JOIN tmp_geo_values l_o1 ON ((ST_Intersects(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(l_o1.strdfgeo)))," +
//                "ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText(l_o2.strdfgeo)))))" +
//                "WHERE l_o2.id = '1610612741'");

//        client.execute("select * from tmp_geo_values limit 1");



    }
}
