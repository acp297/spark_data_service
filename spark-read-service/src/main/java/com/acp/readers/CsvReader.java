package com.acp.readers;

import com.acp.config.ReaderConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class deals with reading data in CSV format.
 * @author Anand Prakash
 */
@Log4j
public class CsvReader extends AbstractReader implements Reader {
    private final ReaderConfig readerConfig;

    public CsvReader(ReaderConfig readerConfig) {
        super(readerConfig);
        this.readerConfig = readerConfig;
    }

    /**
     * Reads the provided csv file and returns dataset.
     *
     * @return Dataset
     */
    @Override
    public Dataset<Row> read() {
        String path = readerConfig.getPath();
        log.info("Reading csv file from : "  + path);
        DataFrameReader dataFrameReader = readerConfig
                .getSparkSession()
                .read();

        setConfig(dataFrameReader);
        return dataFrameReader.csv(path);
    }

    @Override
    void setConfig(DataFrameReader dataFrameReader) {
        setBaseConfig(dataFrameReader);
    }
}
