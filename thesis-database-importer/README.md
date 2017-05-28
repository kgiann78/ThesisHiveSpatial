
Preparing tables to HDFS

In order to add a postgres table as table in the spark sql
 we need to create first a parquet file in the HDFS from
 that table and then import it in the spark.

 To crate a a parquet file in the HDFS run the DatabaseImport
 application with spark-submit. This application gets the following arguments:

-host [address] the host address of the postgres database \
-port [portNumber] the port of the postgres database \
-mode [client | cluster] \
-table [tableName] the selected table \

If we need to convert a specific field from wkb to another form like geoJSON or Text
add the following arguments:

-field [fieldName] the selected field to apply conversion \
-convert [wkt2geoJSON | wkt2text] the conversion method to apply

Example of use:

 spark-submit \\ \
 --class DatabaseImport \\ \
 --master yarn \\ \
 --deploy-mode client \\ \
 /root/thesis-spatial/thesis-database-importer/target/hive-database-importer-jar-with-dependencies.jar \\ \
 -host localhost \\ \
 -port 5432 \\ \
 -mode client \\ \
 -table tmp_geo_values \\ \
 -field strdfgeo \\ \
 -convert wkt2geoJSON