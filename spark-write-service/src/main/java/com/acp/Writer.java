package com.acp;

import org.apache.spark.sql.Dataset;

public interface Writer {
    void write(Dataset ds);
}
