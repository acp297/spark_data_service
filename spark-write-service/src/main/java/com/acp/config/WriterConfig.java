package com.acp.config;

import com.acp.enums.FileTypeEnum;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.SaveMode;

import java.util.Map;

/**
 * Config class set by the WriterBuilder.
 * @author Anand Prakash
 */
@Getter
@Setter
public class WriterConfig {
    private FileTypeEnum writerType;
    private Map<String, String> options;
    private String path;
    private SaveMode mode;
    private String[] partitionByColumns;

    public WriterConfig(FileTypeEnum writerType,
                        Map<String, String> options,
                        String path, SaveMode mode,
                        String[] partitionByColumns) {

        Validate.notNull(writerType, "File format type needs to be set in WriterBuilder");
        Validate.notNull(path, "path needs to be set in WriterBuilder");

        this.writerType = writerType;
        this.options = options;
        this.path = path;
        this.mode = mode;
        this.partitionByColumns = partitionByColumns;
    }
}
