package com.acp.azureSqlDataWarehouse;

import com.acp.Loader;
import com.acp.config.SqlLoaderConfig;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * Loads the respective dataset to data warehouse using azure Polybase mechanism.
 * @author Anand Prakash
 */
public class SqlDwLoader extends AbstractDwLoader implements Loader {

    @Override
    public void init(SqlLoaderConfig sqlLoaderConfig) {
        initializeDwConfig(sqlLoaderConfig);
    }

    /**
     * Loads the dataset to data warehouse.
     * @param ds
     */
    @Override
    public void load(Dataset<Row> ds) {
        DataFrameWriter<Row> dataFrameWriter = ds.write();
        setConfig(dataFrameWriter);
        dataFrameWriter.save();
    }

    /**
     * Sets the required configuration.
     * @param dataFrameWriter
     */
    @Override
    void setConfig(DataFrameWriter<Row> dataFrameWriter) {
        setBaseConfig(dataFrameWriter);
    }
}
