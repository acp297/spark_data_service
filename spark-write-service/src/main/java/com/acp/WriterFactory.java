package com.acp;


import com.acp.config.WriterConfig;
import com.acp.enums.FileTypeEnum;

/**
 * Factory class which maps file format to its corresponding Writer implementation class.
 * @author Anand Prakash
 */
public class WriterFactory {

    /**
     * Based on the File format type, it returns corresponding Writer implementation instance.
     *
     * @param writerConfig
     * @return Writer object.
     */
    public static Writer getWriter(WriterConfig writerConfig) throws IllegalArgumentException{
        FileTypeEnum type = writerConfig.getWriterType();

        switch (type) {
            case CSV:
                return new CsvWriter(writerConfig);

            case AVRO:
                return new AvroWriter(writerConfig);

            case JSON:
                return new JsonWriter(writerConfig);

            case DELTA:
                return new DeltaWriter(writerConfig);

            case PARQUET:
                return new ParquetWriter(writerConfig);
        }
        throw new IllegalArgumentException("Unknown writer type");
    }
}
