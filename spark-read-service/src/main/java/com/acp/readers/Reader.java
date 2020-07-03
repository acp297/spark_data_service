package com.acp.readers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Reader {
    Dataset<Row> read();
}
