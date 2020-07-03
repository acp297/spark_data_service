package com.acp.readers;

import com.acp.config.ReaderConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class deals with reading data in Avro format.
 * @author Anand Prakash
 */
@Log4j
public class AvroReader extends AbstractReader implements Reader {
    private final ReaderConfig readerConfig;

    public AvroReader(ReaderConfig readerConfig) {
        super(readerConfig);
        this.readerConfig = readerConfig;
    }

    /**
     * Reads the provided avro file and returns dataset.
     *
     * @return Dataset
     */
    @Override
    public Dataset<Row> read() {
        String path = readerConfig.getPath();
        log.info("Reading avro file from : " + path);
        DataFrameReader dataFrameReader = readerConfig
                .getSparkSession()
                .read()
                .format(readerConfig.getReaderType().getName());
        setConfig(dataFrameReader);
        return dataFrameReader.load(path);
    }

    @Override
    void setConfig(DataFrameReader dataFrameReader) {
        setBaseConfig(dataFrameReader);
    }
}
