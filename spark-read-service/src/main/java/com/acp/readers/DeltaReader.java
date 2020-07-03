package com.acp.readers;

import com.acp.config.ReaderConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class deals with reading data in Delta format.
 * @author Anand Prakash
 */
@Log4j
public class DeltaReader extends AbstractReader implements Reader {
    private final ReaderConfig readerConfig;

    public DeltaReader(ReaderConfig readerConfig) {
        super(readerConfig);
        this.readerConfig = readerConfig;
    }

    /**
     * Reads the provided delta file and returns dataset.
     *
     * @return Dataset
     */
    @Override
    public Dataset<Row> read() {
        String path = readerConfig.getPath();
        log.info("Reading delta file from : "  + path);
        DataFrameReader dataFrameReader = readerConfig
                .getSparkSession()
                .read()
                .format(readerConfig.getReaderType().getName());

        setConfig(dataFrameReader);
        return dataFrameReader.load(path);
    }

    @Override
    void setConfig(DataFrameReader dataFrameReader)  {
        setBaseConfig(dataFrameReader);
    }
}
