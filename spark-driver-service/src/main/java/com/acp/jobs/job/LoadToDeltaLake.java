package com.acp.jobs.job;

import com.acp.ReaderBuilder;
import com.acp.config.JobConfig;
import com.acp.enums.FileTypeEnum;
import com.acp.readers.Reader;
import com.acp.write.DeltaLakeUpsertWriter;
import com.acp.write.UpsertBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


/***
 * This class performs Upsert to delta lake in ADLS.
 *
 * @author Anand Prakash
 */

@Slf4j
public class LoadToDeltaLake implements Job {
    private JobConfig jobConfig;

    @Override
    public void init(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
    }

    @Override
    public void run() {
        toDelta();
    }

    /**
     * Loads data to delta lake
     * @throws IOException
     */

    public void toDelta() {
        SparkSession sparkSession = jobConfig.getSparkSession();
        FileTypeEnum fileType = FileTypeEnum.valueOf(jobConfig.getSourceFileType().toUpperCase());
        String sourcePath = jobConfig.getSourcePaths()[0];
        String targetPath = jobConfig.getTargetPath();
        String[] joinOnColumns = jobConfig.getPrimaryKeys();
        List<String> partitionColumns = jobConfig.getPartitionColumns();

        log.info("Loading data data to delta lake....");

        Reader reader = ReaderBuilder.builder()
                .sparkSession(sparkSession)
                .readerType(fileType)
                .path(sourcePath)
                .build();

        Dataset<Row> sourceDs = reader.read();
        if (sourceDs.isEmpty()){
            log.info("Empty Dataset found at : " + sourcePath);
            return;
        }
        log.info("Source schema: " + sourceDs.schema().treeString());

        HashMap<String, String> columnMapping = new HashMap<>();
        for (String cols : joinOnColumns){
            columnMapping.put(cols, cols);
        }

        DeltaLakeUpsertWriter transformer = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation(targetPath)
                .setDs(sourceDs)
                .joinOnColumns(columnMapping)
                .partitionColumns(partitionColumns)
                .repartitionValue(jobConfig.getRepartitionValue())
                .build();

        transformer.upsert();
        log.info("Successfully loaded data to delta lake");
    }
}
