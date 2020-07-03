package com.acp;

import com.acp.config.ReaderConfig;
import com.acp.enums.FileTypeEnum;
import com.acp.readers.*;

/**
 * Factory class which maps file format to its corresponding Reader implementation class.
 * @author Anand Prakash
 */
class ReaderFactory {

    /**
     * Based on the File format type, it returns corresponding Reader implementation instance.
     *
     * @param readerConfig
     * @return Reader object.
     */
    static Reader getReader(ReaderConfig readerConfig) throws IllegalArgumentException {
        FileTypeEnum type = readerConfig.getReaderType();

        switch (type) {
            case AVRO:
                return new AvroReader(readerConfig);

            case CSV:
                return new CsvReader(readerConfig);

            case JSON:
                return new JsonReader(readerConfig);

            case DELTA:
                return new DeltaReader(readerConfig);

            case PARQUET:
                return new ParquetReader(readerConfig);
        }
        throw new IllegalArgumentException("Unknown reader type format");
    }
}
