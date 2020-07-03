package com.acp.write.config;

import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * Config class used for Upsert.
 * @author Anand Prakash
 */
@Getter
public class UpsertConfig {
    private SparkSession sparkSession;
    private Dataset<Row> ds;
    private Map<String, String> options;
    private Map<String, String> columnsToInsert;
    private Map<String, String> columnsToUpsert;
    private Map<String, String> joinOnColumns;
    private String deltaLocation;
    private List<String> partitionColumns;
    private int repartitionValue;
    private List<String> sortColumns;

    public UpsertConfig(SparkSession sparkSession, Dataset<Row> ds,
                        Map<String, String> columnsToInsert,
                        Map<String, String> columnsToUpsert,
                        Map<String, String> joinOnColumns,
                        Map<String, String> options,
                        String deltaLocation,
                        List<String> partitionColumns,
                        int repartitionValue,
                        List<String> sortColumns) {

        Validate.notNull(sparkSession, "Spark session needs to be set in ReaderBuilder");
        Validate.notNull(ds, "Dataset needs to be set in ReaderBuilder");
        Validate.notNull(joinOnColumns, "Join on columns needs to be set in ReaderBuilder");
        Validate.notNull(deltaLocation, "delta location needs to be set in ReaderBuilder");

        this.sparkSession = sparkSession;
        this.ds = ds;
        this.columnsToInsert = columnsToInsert;
        this.columnsToUpsert = columnsToUpsert;
        this.joinOnColumns = joinOnColumns;
        this.options = options;
        this.deltaLocation = deltaLocation;
        this.partitionColumns = partitionColumns;
        this.repartitionValue = repartitionValue;
        this.sortColumns = sortColumns;
    }
}
