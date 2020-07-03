package com.acp;

import com.acp.config.WriterConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;


/**
 * Class deals with reading data in csv format.
 * @author Anand Prakash
 */
@Log4j
public class CsvWriter extends AbstractWriter implements Writer {
    private final WriterConfig writerConfig;

    public CsvWriter(WriterConfig writerConfig) {
        super(writerConfig);
        this.writerConfig = writerConfig;
    }

    /**
     * This method takes the dataset and writes it in CSV format.
     * @param ds Dataset
     */
    @Override
    public void write(Dataset ds) {
        String path = writerConfig.getPath();
        log.info("Writing csv file to : " + path);
        DataFrameWriter dataFrameWriter = ds.write();
        setConfig(dataFrameWriter);

        dataFrameWriter.csv(path);
    }

    @Override
    void setConfig(DataFrameWriter dataFrameWriter) {
        setBaseConfig(dataFrameWriter);
    }
}