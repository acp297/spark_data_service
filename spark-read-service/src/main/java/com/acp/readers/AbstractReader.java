package com.acp.readers;

import com.acp.config.ReaderConfig;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Abstract class to set the base properties of the Reader.
 * @author Anand Prakash
 */
abstract class AbstractReader {
    private ReaderConfig readerConfig;

    public AbstractReader(ReaderConfig readerConfig) {
        this.readerConfig = readerConfig;
    }

    abstract void setConfig(DataFrameReader dataFrameReader);

    void setBaseConfig(DataFrameReader dataFrameReader) {
        setSchema(dataFrameReader, readerConfig.getSchema());
        setOptions(dataFrameReader, readerConfig.getOptions());
    }

    /**
     * Set Schema if available.
     * @param dataFrameReader
     * @param schema
     */
    private void setSchema(DataFrameReader dataFrameReader, StructType schema) {
        if (schema != null) {
            dataFrameReader.schema(schema);
        }
    }

    /**
     * Set options properties if available.
     * @param dataFrameReader
     * @param options
     */
    private void setOptions(DataFrameReader dataFrameReader, Map<String, String> options) {
        if (options != null && !options.isEmpty()) {
            dataFrameReader.options(options);
        }
    }

}
