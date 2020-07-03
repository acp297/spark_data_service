package com.acp;

import com.acp.config.WriterConfig;
import com.acp.enums.FileTypeEnum;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

/**
 * Builder class for Writer.
 * @author Anand Prakash
 */
public class WriterBuilder {
    private FileTypeEnum writerType;
    private Map<String, String> options;
    private String path;
    private SaveMode mode;
    private String[] partitionBy;


    private WriterBuilder(){}

    public static WriterBuilder builder() {
        return new WriterBuilder();
    }

    /**
     * Setter to set file format.
     *
     * @param writerType
     * @return WriterBuilder object.
     */
    public WriterBuilder writerType(FileTypeEnum writerType) {
        this.writerType = writerType;
        return this;
    }

    /**
     * Setter to set options which is used as config by spark to read.
     *
     * @param options
     * @return WriterBuilder object.
     */
    public WriterBuilder options(Map<String, String> options) {
        this.options = options;
        return this;
    }

    /**
     * Setter to set file path that needs to be read.
     *
     * @param path
     * @return WriterBuilder object.
     */
    public WriterBuilder path(String path) {
        this.path = path;
        return this;
    }

    /**
     * Setter to set Savemode.
     *
     * @param mode
     * @return WriterBuilder object.
     */
    public WriterBuilder mode(SaveMode mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Setter to set the partition by based on columns.
     * @param partitionBy
     * @return
     */
    public WriterBuilder partitionBy(String[] partitionBy) {
        this.partitionBy = partitionBy;
        return this;
    }

    /**
     * This method will return the corresponding writer object based on file format.
     *
     * @return Writer object.
     */
    public Writer build() {
        return WriterFactory.getWriter(
                new WriterConfig(
                        this.writerType, this.options, this.path, this.mode, this.partitionBy));
    }
}
