package com.acp;

import com.acp.config.WriterConfig;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

/**
 * Abstract class to set the base properties of the Writer.
 * @author Anand Prakash
 */
abstract class AbstractWriter {
    private WriterConfig writerConfig;

    AbstractWriter(WriterConfig writerConfig) {
        this.writerConfig = writerConfig;
    }

    abstract void setConfig(DataFrameWriter dataFrameWriter);

    public void setBaseConfig(DataFrameWriter dataFrameWriter) {
        setOptions(dataFrameWriter, writerConfig.getOptions());
        setSaveMode(dataFrameWriter, writerConfig.getMode());
        partitionBy(dataFrameWriter, writerConfig.getPartitionByColumns());
    }


    /**
     * Adds output options for the underlying data source.
     * @param dataFrameWriter
     * @param options
     */
    private void setOptions(DataFrameWriter dataFrameWriter, Map<String, String> options) {
        if (options != null && !options.isEmpty()) {
            dataFrameWriter.options(options);
        }
    }

    /**
     * Partitions the output by the given columns on the file system.
     * @param dataFrameWriter
     * @param columns
     */
    private void partitionBy(DataFrameWriter dataFrameWriter, String[] columns) {
        if (ArrayUtils.isNotEmpty(columns)) {
            dataFrameWriter.partitionBy(columns);
        }
    }

    /**
     * Specifies the behavior when data or table already exists.
     * @param dataFrameWriter
     * @param saveMode
     */
    private void setSaveMode(DataFrameWriter dataFrameWriter, SaveMode saveMode) {
        if (saveMode != null){
            dataFrameWriter.mode(saveMode);
        }
    }
}
