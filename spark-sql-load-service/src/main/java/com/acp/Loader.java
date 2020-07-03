package com.acp;

import com.acp.config.SqlLoaderConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Loader {
    void load(Dataset<Row> ds);
    void init(SqlLoaderConfig sqlLoaderConfig);
}
