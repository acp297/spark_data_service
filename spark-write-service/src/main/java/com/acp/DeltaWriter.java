package com.acp;

import com.acp.config.WriterConfig;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;

/**
 * Class deals with reading data in Delta format.
 * @author Anand Prakash
 */
@Log4j
public class DeltaWriter extends AbstractWriter implements Writer {
    private final WriterConfig writerConfig;

    public DeltaWriter(WriterConfig writerConfig) {
        super(writerConfig);
        this.writerConfig = writerConfig;
    }

    /**
     * This method takes the dataset and writes it in Delta format.
     * @param ds Dataset
     */
    @Override
    public void write(Dataset ds) {
        String path = writerConfig.getPath();
        log.info("Writing delta file to : " + path);
        DataFrameWriter dataFrameWriter = ds.write()
                .format(writerConfig.getWriterType().getName());

        setConfig(dataFrameWriter);
        dataFrameWriter.save(path);
    }

    @Override
    void setConfig(DataFrameWriter dataFrameWriter) {
        setBaseConfig(dataFrameWriter);
    }
}
