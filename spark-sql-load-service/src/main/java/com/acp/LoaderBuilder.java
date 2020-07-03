package com.acp;

import com.acp.config.SqlLoaderConfig;
import com.acp.enums.SqlLoaderType;
import com.acp.models.ConnectionConfig;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

/**
 * Sql loader builder class.
 * @author Anand Prakash
 */
public class LoaderBuilder {
    private Map<String, String> options;
    private SaveMode mode;
    private String table;
    private String query;
    private ConnectionConfig connectionConfig;
    private SqlLoaderType sqlLoaderType;
    private Boolean createTable;


    private LoaderBuilder(){}

    public static LoaderBuilder builder(){
        return new LoaderBuilder();
    }

    public LoaderBuilder options(Map<String, String> options) {
        this.options = options;
        return this;
    }

    public LoaderBuilder mode(SaveMode mode) {
        this.mode = mode;
        return this;
    }

    public LoaderBuilder table(String table) {
        this.table = table;
        return this;
    }

    public LoaderBuilder query(String query) {
        this.query = query;
        return this;
    }

    public LoaderBuilder connectionConfig(
            ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    public LoaderBuilder sqlLoaderType(SqlLoaderType sqlLoaderType) {
        this.sqlLoaderType = sqlLoaderType;
        return this;
    }

    public LoaderBuilder createTable(Boolean createTable) {
        this.createTable = createTable;
        return this;
    }

    public Loader build(){
        Loader loader = LoaderFactory.getLoader(sqlLoaderType);
        loader.init(new SqlLoaderConfig(connectionConfig,
                this.options, this.mode, this.table, this.query, this.createTable));
        return loader;
    }
}
