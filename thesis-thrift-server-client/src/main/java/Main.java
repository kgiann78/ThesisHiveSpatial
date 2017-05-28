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
        client.execute("CREATE EXTERNAL TABLE tmp_geo_values " +
                "(id int, srid int, strdfgeo  binary) " +
                " STORED AS PARQUET LOCATION " +
                "'hdfs:///tmp_geo_values_parquet'");
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

//
//
//        try {
//            Class.forName(driverName);
//        } catch (ClassNotFoundException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//            System.exit(1);
//        }
//        try {
//            Connection con = DriverManager.getConnection("jdbc:hive2://" + host + ":10000", "constantine", "");
//            Statement stmt = con.createStatement();
////            String tablename = "earthquakes";
////            String sql = "select * from " + tablename;
////            ResultSet res = stmt.executeQuery(sql);
////            while (res.next()) {
////                System.out.println(res.getString(1));
////            }
//
//            // Create table
//            String tableName = "testHiveDriverTable";
//            String sql = "drop table if exists " + tableName;
//            System.out.println("Running: " + sql);
//            stmt.executeQuery(sql);
//            sql = "create table " + tableName + " (key int, value string, comment string) " +
//                    "row format delimited fields terminated by ',' STORED AS TEXTFILE";
//            System.out.println("Running: " + sql);
//            stmt.executeQuery(sql);
//
//            // show tables
//            sql = "show tables '" + tableName + "'";
//            System.out.println("Running: " + sql);
//            ResultSet res = stmt.executeQuery(sql);
//            if (res.next()) {
//                System.out.println(res.getString(1));
//            }
//
//            // describe table
//            sql = "describe " + tableName;
//            System.out.println("Running: " + sql);
//            res = stmt.executeQuery(sql);
//            while (res.next()) {
//                System.out.println(res.getString(1) + "\t" + res.getString(2));
//            }
//
////            // load data into table
////            sql = "load data local inpath '" + filepath + "' into table " + tableName;
////            System.out.println("Running: " + sql);
////            stmt.executeUpdate(sql);
//
//            // select * query
//            sql = "select * from " + tableName;
//            System.out.println("Running: " + sql);
//            res = stmt.executeQuery(sql);
//            while (res.next()) {
//                System.out.println(res.getInt(1) + "\t" + res.getString(2) + "\t" + res.getString(3));
//            }
//
//            // regular hive query
//            sql = "select count(1) from " + tableName;
//            System.out.println("Running: " + sql);
//            res = stmt.executeQuery(sql);
//            while (res.next()) {
//                System.out.println(res.getString(1));
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }
}
