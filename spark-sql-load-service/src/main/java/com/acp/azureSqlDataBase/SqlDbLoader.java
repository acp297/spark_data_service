package com.acp.azureSqlDataBase;

import com.acp.Loader;
import com.acp.config.SqlLoaderConfig;
import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata;
import com.microsoft.azure.sqldb.spark.config.SqlDBConfigBuilder;
import com.microsoft.azure.sqldb.spark.connect.DataFrameFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Loads the respective dataset to data base using azure bulk copy mechanism.
 * @author Anand Prakash
 */
public class SqlDbLoader extends AbstractDbLoader implements Loader {
    /**
     * Initialize SqlLoaderConfig.
     * @param sqlLoaderConfig
     */
    @Override
    public void init(SqlLoaderConfig sqlLoaderConfig) {
        initializeDbConfig(sqlLoaderConfig);
    }

    /**
     * Parent method to initiate bulk copy activity.
     * @param ds
     */
    @Override
    public void load(Dataset<Row> ds) {
        bulkLoad(ds);
    }

    /**
     * Performs bulk copy activity to Azure sql database.
     * @param ds
     */
    private void bulkLoad(Dataset<Row> ds) {
        DataFrameFunctions<Dataset<Row>> dfFunc = new DataFrameFunctions<>(ds);
        Map<String, Object> configMap = new HashMap<>();
        setConfig(configMap);

        SqlDBConfigBuilder configBuilder = new SqlDBConfigBuilder(JavaConverters
                .mapAsScalaMapConverter(configMap)
                .asScala().toMap(Predef.<Tuple2<String, Object>>conforms()));

        BulkCopyMetadata bulkCopyMetadata = new BulkCopyMetadata();
        int count = 1;
        for (StructField structField : ds.schema().fields()){
            String dataType = structField.dataType().typeName();
            int javaSqlDataTypeCode = getJavaSqlDataTypeCode(dataType);
            int precision = dataTypePrecisionMapping().get(javaSqlDataTypeCode);
            bulkCopyMetadata.addColumnMetadata(count++, structField.name(),
                    javaSqlDataTypeCode, precision, 10);
        }
        dfFunc.bulkCopyToSqlDB(configBuilder.build(),bulkCopyMetadata, createTable);
    }

    /**
     * Sets config req for bulk copy load.
     * @param config
     */
    @Override
    void setConfig(Map<String, Object> config) {
        setBaseConfigForBulkLoad(config);
    }

    /**
     * Gets java.sql data type based on struct field data type.
     * @param dataType
     * @return
     */
    int getJavaSqlDataTypeCode(String dataType) {

        if (Pattern.matches("decimal.*", dataType)) {
            dataType = "decimal";
        }

        switch (dataType) {
            case "string":
                return Types.VARCHAR;

            case "integer":
                return Types.INTEGER;

            case "long":
                return Types.BIGINT;

            case "date":
                return Types.DATE;

            case "timestamp":
                return Types.TIMESTAMP;

            case "boolean":
                return Types.BOOLEAN;

            case "double":
                return Types.DOUBLE;

            case "float":
                return Types.FLOAT;

            case "decimal":
                return Types.DECIMAL;

            case "short":
                return Types.SMALLINT;

            case "null":
                return Types.NULL;

            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }
    }

    /**
     * Map of DataType vs its max precision.
     * @return
     */
    private Map<Integer, Integer> dataTypePrecisionMapping(){
        Map<Integer, Integer> dataTypePrecisionMap = new HashMap<>();
        dataTypePrecisionMap.put(Types.VARCHAR, 8000);
        dataTypePrecisionMap.put(Types.INTEGER, 38);
        dataTypePrecisionMap.put(Types.BIGINT, 38);
        dataTypePrecisionMap.put(Types.DATE, 50);
        dataTypePrecisionMap.put(Types.TIMESTAMP, 50);
        dataTypePrecisionMap.put(Types.BOOLEAN, 38);
        dataTypePrecisionMap.put(Types.DOUBLE, 38);
        dataTypePrecisionMap.put(Types.FLOAT, 38);
        dataTypePrecisionMap.put(Types.DECIMAL, 38);
        dataTypePrecisionMap.put(Types.SMALLINT, 38);
        dataTypePrecisionMap.put(Types.NULL, 50);
        return dataTypePrecisionMap;
    }
}
