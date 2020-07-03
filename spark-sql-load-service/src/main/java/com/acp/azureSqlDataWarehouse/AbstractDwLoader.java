package com.acp.azureSqlDataWarehouse;

import com.acp.config.SqlLoaderConfig;
import com.acp.constants.SparkOptions;
import com.acp.constants.SqlConstants;
import com.acp.models.ConnectionConfig;
import org.apache.commons.collections.MapUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.Map;
import java.util.Optional;


/**
 * This is an abstract class which generates sql connection string.
 * Set necessary configuration to dataFrameWriter.
 * @author Anand Prakash
 */

public abstract class AbstractDwLoader {

    private SqlLoaderConfig sqlLoaderConfig;
    private ConnectionConfig connectionConfig;

    public void initializeDwConfig(SqlLoaderConfig sqlLoaderConfig) {
        this.sqlLoaderConfig = sqlLoaderConfig;
        connectionConfig = sqlLoaderConfig.getConnectionConfig();
    }

    abstract void setConfig(DataFrameWriter<Row> dataFrameWriter);

    public void setBaseConfig(DataFrameWriter<Row> dataFrameWriter) {
        dataFrameWriter.format(connectionConfig.getFormat());
        dataFrameWriter.option(SparkOptions.MAX_STRING_LENGTH,
                SparkOptions.MAX_STRING_LENGTH_VALUE);
        dataFrameWriter.option(SparkOptions.URL, generateConnectionString());


        if (Optional.ofNullable(sqlLoaderConfig.getTable()).isPresent()){
            dataFrameWriter.option(SparkOptions.DB_TABLE, sqlLoaderConfig.getTable());
        }
        else if (Optional.ofNullable(sqlLoaderConfig.getQuery()).isPresent()){
            dataFrameWriter.option(SparkOptions.QUERY, sqlLoaderConfig.getQuery());
        }
        setOptions(dataFrameWriter, sqlLoaderConfig.getOptions());
        setSaveMode(dataFrameWriter, sqlLoaderConfig.getMode());
    }

    /**
     * Generates and returns a connection string for SQL DW
     *
     * @return
     */
    private String generateConnectionString() {
        final String SEMI_COLON = ";";
        final String EQUAL = "=";
        final String ENCRYPT = "encrypt";
        final String TRUST_SERVER_CERTIFICATE = "trustServerCertificate";
        final String HOST_NAME_IN_CERTIFICATE = "hostNameInCertificate";
        final String LOG_IN_TIME_OUT = "loginTimeout";

        StringBuilder connectionString = new StringBuilder();
        return connectionString
                .append(connectionConfig.getJdbcUrl())
                .append(SEMI_COLON)
                .append(SqlConstants.DATABASE).append(EQUAL).append(connectionConfig.getDatabase())
                .append(SEMI_COLON)
                .append(SqlConstants.USER).append(EQUAL).append(connectionConfig.getUsername())
                .append(SEMI_COLON)
                .append(SqlConstants.PASSWORD).append(EQUAL).append(connectionConfig.getPassword())
                .append(SEMI_COLON)
                .append(ENCRYPT).append(EQUAL).append(connectionConfig.getEncrypt())
                .append(SEMI_COLON)
                .append(TRUST_SERVER_CERTIFICATE).append(EQUAL)
                .append(connectionConfig.getTrustServerCertificate())
                .append(SEMI_COLON)
                .append(HOST_NAME_IN_CERTIFICATE).append(EQUAL)
                .append(connectionConfig.getHostNameInCertificate())
                .append(SEMI_COLON)
                .append(LOG_IN_TIME_OUT).append(EQUAL).append(connectionConfig.getLoginTimeout())
                .toString();
    }

    /**
     * Adds output options for the underlying data source.
     * @param dataFrameWriter
     * @param options
     */
    private void setOptions(DataFrameWriter<Row> dataFrameWriter, Map<String, String> options) {
        if (MapUtils.isNotEmpty(options)) {
            dataFrameWriter.options(options);
        }
    }

    /**
     * Specifies the behavior when data or table already exists.
     * @param dataFrameWriter
     * @param saveMode
     */
    private void setSaveMode(DataFrameWriter<Row> dataFrameWriter, SaveMode saveMode) {
        if (Optional.ofNullable(saveMode).isPresent()){
            dataFrameWriter.mode(saveMode);
        }
    }
}
