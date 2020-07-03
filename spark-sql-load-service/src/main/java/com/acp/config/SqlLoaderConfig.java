package com.acp.config;

import com.acp.models.ConnectionConfig;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

/**
 * Config class set by the SqlWriterBuilder.
 * @author Anand Prakash
 */
@Getter
public class SqlLoaderConfig {

    private ConnectionConfig connectionConfig;
    private Map<String, String> options;
    private SaveMode mode;
    private String table;
    private String query;
    private Boolean createTable;

    public SqlLoaderConfig(ConnectionConfig connectionConfig,
                           Map<String, String> options,
                           SaveMode mode, String table,
                           String query, Boolean createTable) {

        Validate.notNull(connectionConfig, "ConnectionConfig cannot be null");
        Validate.notNull(table, "table name cannot be null");

        this.connectionConfig = connectionConfig;
        this.options = options;
        this.mode = mode;
        this.table = table;
        this.query = query;
        this.createTable = createTable;
    }
}
