import java.sql.*;
import java.util.Timer;

public class Main {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {

        String host = "snf-649502.vm.okeanos.grnet.gr";
        String filepath = "/Users/constantine/Development/IdeaProjects/SparkHiveClient/src/main/resources/data.txt";

        ThriftServerClient client = new ThriftServerClient(host, "constantine", "");

//        client.execute("show databases");
//        client.execute("show tables");

        /*
        client.execute("DROP TABLE IF EXISTS geo_values");
        client.execute("DROP TABLE IF EXISTS uri_values");
        client.execute("DROP TABLE IF EXISTS aswkt_235007");
        client.execute("DROP TABLE IF EXISTS type_180829");
        client.execute("DROP TABLE IF EXISTS type_855540");

        client.execute("CREATE EXTERNAL TABLE aswkt_235007 " +
                "(ctx int, subj int, obj int, expl boolean) " +
                "STORED AS PARQUET LOCATION " +
                "'hdfs:///aswkt_235007_parquet'");

        client.execute("CREATE EXTERNAL TABLE type_180829 " +
                "(ctx int, subj int, obj int, expl boolean) " +
                "STORED AS PARQUET LOCATION " +
                "'hdfs:///type_180829_parquet'");

        client.execute("CREATE EXTERNAL TABLE type_855540 " +
                "(ctx int, subj int, obj int, expl boolean) " +
                " STORED AS PARQUET LOCATION " +
                "'hdfs:///type_855540_parquet'");


        client.execute("CREATE TABLE geo_values" +
                "(id int, srid int, strdfgeo string) " +
                " STORED AS PARQUET LOCATION " +
                "'hdfs:///tmp_geo_values_parquet'");

        client.execute("CREATE TABLE uri_values" +
                "(id int, value string) " +
                " STORED AS PARQUET LOCATION " +
                "'hdfs:///uri_values_parquet'");

        client.execute("select count(*) from aswkt_235007");
        client.execute("select count(*) from type_180829");
        client.execute("select count(*) from type_855540");
        client.execute("select count(*) from geo_values");
        client.execute("select count(*) from uri_values");
        client.execute("select count(*) from type_855540 t4 where t4.obj = '805404980'");
        client.execute("select count(*) from type_180829 t5 where t5.obj = '805404980'");
*/
//        client.execute("select count(*) from geo_values");


        client.execute("SELECT t0.subj, u_s2.value, a3.subj, u_s1.value " +
//        client.execute("SELECT count(*) " +
                "FROM type_180829 t0 " +
                "INNER JOIN aswkt_235007 a1 ON (a1.subj = t0.subj) " +
                "INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj) " +
                "INNER JOIN geo_values l_o1 ON ((ST_Intersects(l_o1.strdfgeo,l_o2.strdfgeo))) " +
                "INNER JOIN aswkt_235007 a3 ON (a3.obj = l_o1.id) " +
                "INNER JOIN type_855540 t4 ON (t4.subj = a3.subj) " +
                "INNER JOIN type_180829 t5 ON (t5.subj = a3.subj) " +
                "LEFT JOIN uri_values u_s2 ON (u_s2.id = t0.subj) " +
                "LEFT JOIN uri_values u_s1 ON (u_s1.id = a3.subj) " +
                "limit 1000"
        );
//        client.execute("SELECT t0.subj,\n" +
//                " u_s2.value,\n" +
//                " a3.subj,\n" +
//                " u_s1.value\n" +
//                "FROM type_180829 t0\n" +
//                " INNER JOIN aswkt_235007 a1 ON (a1.subj = t0.subj)\n" +
//                " INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)\n" +
//                " INNER JOIN geo_values l_o1 ON ((ST_Intersects(l_o1.strdfgeo,l_o2.strdfgeo)))\n" +
//                " INNER JOIN aswkt_235007 a3 ON (a3.obj = l_o1.id)\n" +
//                " INNER JOIN type_855540 t4 ON (t4.obj =  '805404980'\n" +
//                " AND t4.subj = a3.subj)\n" +
//                " INNER JOIN type_180829 t5 ON (t5.obj =  '805404980'\n" +
//                " AND t5.subj = a3.subj)\n" +
//                " LEFT JOIN uri_values u_s2 ON (u_s2.id = t0.subj)\n" +
//                " LEFT JOIN uri_values u_s1 ON (u_s1.id = a3.subj)\n" +
//                " WHERE t5.obj = '180828'\n" +
//                " limit 10");
/*
    EXPLAIN ANALYZE SELECT t0.subj,
     u_s2.value,
     a3.subj,
     u_s1.value
    FROM type_180829 t0
     INNER JOIN aswkt_235040 a1 ON (a1.subj = t0.subj)
     INNER JOIN geo_values l_o2 ON (l_o2.id = a1.obj)
     INNER JOIN geo_values l_o1 ON ((ST_Intersects(l_o1.strdfgeo,l_o2.strdfgeo)))
     INNER JOIN aswkt_235040 a3 ON (a3.obj = l_o1.id)
     INNER JOIN type_855540 t4 ON (t4.obj =  '805404980'
     AND t4.subj = a3.subj)
     INNER JOIN type_180829 t5 ON (t5.obj =  '805404980'
     AND t5.subj = a3.subj)
     LEFT JOIN uri_values u_s2 ON (u_s2.id = t0.subj)
     LEFT JOIN uri_values u_s1 ON (u_s1.id = a3.subj)
 */
    }
}
