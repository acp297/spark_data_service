package com.acp;

import com.acp.config.WriterConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;

/**
 * Class deals with reading data in Parquet format.
 * @author Anand Prakash
 */
@Log4j
public class ParquetWriter extends AbstractWriter implements Writer {
    private final WriterConfig writerConfig;

    public ParquetWriter(WriterConfig writerConfig) {
        super(writerConfig);
        this.writerConfig = writerConfig;
    }

    /**
     * This method takes the dataset and writes it in Parquet format.
     * @param ds Dataset
     */
    @Override
    public void write(Dataset ds) {
        String path = writerConfig.getPath();
        log.info("Writing parquet file to : " + path);

        DataFrameWriter dataFrameWriter = ds.write();
        setConfig(dataFrameWriter);
        dataFrameWriter.parquet(path);
    }

    @Override
    void setConfig(DataFrameWriter dataFrameWriter) {
        setBaseConfig(dataFrameWriter);
    }
}
