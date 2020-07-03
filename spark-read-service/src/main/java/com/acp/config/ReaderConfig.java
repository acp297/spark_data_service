package com.acp.config;

import com.acp.enums.FileTypeEnum;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Config class set by the ReaderBuilder.
 * @author Anand Prakash
 */
@Getter
@Setter
public class ReaderConfig {
    private SparkSession sparkSession;
    private FileTypeEnum readerType;
    private Map<String, String> options;
    private String path;
    private StructType schema;

    public ReaderConfig(SparkSession sparkSession,
                        FileTypeEnum readerType,
                        Map<String, String> options,
                        String path, StructType schema) {

        Validate.notNull(sparkSession, "Spark session needs to be set in ReaderBuilder");
        Validate.notNull(readerType, "File format type needs to be set in ReaderBuilder");
        Validate.notNull(path, "path needs to be set in ReaderBuilder");

        this.sparkSession = sparkSession;
        this.readerType = readerType;
        this.options = options;
        this.path = path;
        this.schema = schema;
    }
}
