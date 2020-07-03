package com.acp.azureSqlDataBase;


import com.acp.config.SqlLoaderConfig;
import com.acp.constants.SqlConstants;
import com.acp.models.ConnectionConfig;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

/**
 * Abstract class to set the base properties of the database Loader.
 * @author Anand Prakash
 */
abstract class AbstractDbLoader {
    public static final String NAME = "name";
    public static final String DB_TABLE = "dbTable";
    public static final String BULK_COPY_TABLE_LOCK = "bulkCopyTableLock";
    public static final String BULK_COPY_TIMEOUT = "bulkCopyTimeout";
    public static final String BULK_COPY_BATCH_SIZE = "bulkCopyBatchSize";
    public static final String BULK_SIZE = "5000";
    public static final String TRUE = "true";
    public static final String TIME_OUT = "6000";
    public boolean createTable;
    private SqlLoaderConfig sqlLoaderConfig;
    private ConnectionConfig connectionConfig;

    public void initializeDbConfig(SqlLoaderConfig sqlLoaderConfig) {
        this.sqlLoaderConfig = sqlLoaderConfig;
        connectionConfig = sqlLoaderConfig.getConnectionConfig();
    }

    abstract void setConfig(Map<String, Object> config);

    public void setBaseConfigForBulkLoad(Map<String, Object> config) {

        config.put(SqlConstants.URL, connectionConfig.getUrl());
        config.put(SqlConstants.DATABASE.concat(NAME), connectionConfig.getDatabase());
        config.put(SqlConstants.USER, connectionConfig.getUsername());
        config.put(SqlConstants.PASSWORD, connectionConfig.getPassword());
        config.put(DB_TABLE, sqlLoaderConfig.getTable());
        setOptions(config, sqlLoaderConfig.getOptions());
        if (sqlLoaderConfig.getCreateTable() != null){
            createTable = sqlLoaderConfig.getCreateTable();
        }
    }

    /**
     * Adds additional options.
     * @param options
     */
    private void setOptions(Map<String, Object> config, Map<String, String> options) {
        if (MapUtils.isEmpty(options)) {
            return;
        }
        if (options.get(BULK_COPY_BATCH_SIZE) != null){
            config.put(BULK_COPY_BATCH_SIZE, options.get(BULK_COPY_BATCH_SIZE));
        } else {
            config.put(BULK_COPY_BATCH_SIZE, BULK_SIZE);
        }
        if (options.get(BULK_COPY_TABLE_LOCK) != null){
            config.put(BULK_COPY_TABLE_LOCK, options.get(BULK_COPY_TABLE_LOCK));
        } else {
            config.put(BULK_COPY_TABLE_LOCK, TRUE);
        }
        if (options.get(BULK_COPY_TIMEOUT) != null){
            config.put(BULK_COPY_TIMEOUT, options.get(BULK_COPY_TIMEOUT));
        } else {
            config.put(BULK_COPY_TIMEOUT, TIME_OUT);
        }
    }
}
