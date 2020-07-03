package com.acp;

import com.acp.config.ReaderConfig;
import com.acp.enums.FileTypeEnum;
import com.acp.readers.Reader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Builder class for Reader.
 * @author Anand Prakash
 */
public class ReaderBuilder {

    private SparkSession sparkSession;
    private FileTypeEnum readerType;
    private Map<String, String> options;
    private String path;
    private StructType schema;

    private ReaderBuilder(){}

    public static ReaderBuilder builder() {
        return new ReaderBuilder();
    }

    /**
     * Set the spark session.
     *
     * @param sparkSession
     * @return ReaderBuilder object.
     */
    public ReaderBuilder sparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        return this;
    }

    /**
     * Setter to set file format.
     *
     * @param readerType
     * @return ReaderBuilder object.
     */
    public ReaderBuilder readerType(FileTypeEnum readerType) {
        this.readerType = readerType;
        return this;
    }

    /**
     * Setter to set options which is used as config by spark to read.
     *
     * @param options
     * @return ReaderBuilder object.
     */
    public ReaderBuilder options(Map<String, String> options) {
        this.options = options;
        return this;
    }

    /**
     * Setter to set file path that needs to be read.
     *
     * @param path
     * @return ReaderBuilder object
     */
    public ReaderBuilder path(String path) {
        this.path = path;
        return this;
    }

    /**
     * Setter to set schema.
     *
     * @param schema
     * @return ReaderBuilder object.
     */
    public ReaderBuilder schema(StructType schema) {
        this.schema = schema;
        return this;
    }

    /**
     * This method will return the corresponding reader object based on file format.
     *
     * @return Reader
     */
    public Reader build() {
        return ReaderFactory.getReader(
                new ReaderConfig(sparkSession, readerType, options, path, schema));
    }
}
