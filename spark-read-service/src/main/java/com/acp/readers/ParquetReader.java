package com.acp.readers;

import com.acp.config.ReaderConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class deals with reading data in Parquet format.
 * @author Anand Prakash
 */
@Log4j
public class ParquetReader extends AbstractReader implements Reader {
    private final ReaderConfig readerConfig;

    public ParquetReader(ReaderConfig readerConfig) {
        super(readerConfig);
        this.readerConfig = readerConfig;
    }

    /**
     * Reads the provided parquet file and returns dataset.
     *
     * @return Dataset
     */
    @Override
    public Dataset<Row> read() {
        String path = readerConfig.getPath();
        log.info("Reading parquet file from : "  + path);
        DataFrameReader dataFrameReader = readerConfig
                .getSparkSession()
                .read();

        setConfig(dataFrameReader);
        return dataFrameReader.parquet(path);
    }

    @Override
    void setConfig(DataFrameReader dataFrameReader) {
        setBaseConfig(dataFrameReader);
    }
}
