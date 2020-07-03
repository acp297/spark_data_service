# spark_data_service

TODO
## Spark Utility project which does various operation with handy library.

TODO
- Can read any source format.
- Can write to any source format.
- Can perform insert on Azure SQL Datawarehouse and database.
- Can Perform insert and update on DELTA LAKE



```--env local --jobType LOAD_TO_DELTALAKE --sourceFileType parquet --sourcePath /adls/source --targetPath /adls/target/ --primaryKeys pk1,pk2 --partitionColumns <Comma list of partition columns : Optional>```

```--env local --jobType LOAD_TO_MS_SQL_DW --sourceFileType delta --sourcePath /deltaLocation --schemaName schemaName --tableName tableName --primaryKeys pk1,pk2 --finalColumns col1,col2```
