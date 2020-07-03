package com.acp.write;

import com.acp.write.config.UpsertConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * Builder class for Upsert.
 * @author Anand Prakash
 */
public class UpsertBuilder {
    private SparkSession sparkSession;
    private Dataset<Row> ds;
    private Map<String, String> columnsToInsert;
    private Map<String, String> columnsToUpsert;
    private Map<String, String> joinOnColumns;
    private String deltaLocation;
    private Map<String, String> options;
    private List<String> partitionColumns;
    private int repartitionValue;
    private List<String> sortColumns;

    private UpsertBuilder(){}

    public static UpsertBuilder builder() {
        return new UpsertBuilder();
    }

    /**
     * Setter to set the SparkSession.
     * @param sparkSession
     * @return TransformerBuilder
     */
    public UpsertBuilder sparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        return this;
    }

    /**
     * Setter to set the dataset.
     * @param ds
     * @return TransformerBuilder
     */
    public UpsertBuilder setDs(Dataset<Row> ds) {
        this.ds = ds;
        return this;
    }

    /**
     * Setter to set the columns mapping between source and target table while insert.
     * @param columnsToInsert
     * @return TransformerBuilder
     */
    public UpsertBuilder columnsToInsert(Map<String, String> columnsToInsert) {
        this.columnsToInsert = columnsToInsert;
        return this;
    }

    /**
     * Setter to set the columns mapping between source and target table while upsert.
     * @param columnsToUpsert
     * @return TransformerBuilder
     */
    public UpsertBuilder columnsToUpsert(Map<String, String> columnsToUpsert) {
        this.columnsToUpsert = columnsToUpsert;
        return this;
    }


    /**
     * Setter to set the columns mapping for join.
     * @param joinOnColumns
     * @return TransformerBuilder
     */
    public UpsertBuilder joinOnColumns(Map<String, String> joinOnColumns) {
        this.joinOnColumns = joinOnColumns;
        return this;
    }

    /**
     * Setter to set the location of the delta file.
     * @param
     * @return
     */
    public UpsertBuilder deltaLocation(String deltaLocation) {
        this.deltaLocation = deltaLocation;
        return this;
    }

    /**
     * Setter to set options which is used as config by spark to upsert.
     *
     * @param options
     * @return WriterBuilder object.
     */
    public UpsertBuilder options(Map<String, String> options) {
        this.options = options;
        return this;
    }

    /**
     * Setting partition columns for upsert
     * @param partitionColumns list of column names
     * @return
     */
    public UpsertBuilder partitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    /**
     * Setting repartition value to create in-memory partition of dataset
     * @param repartitionValue
     * @return
     */
    public UpsertBuilder repartitionValue(int repartitionValue) {
        this.repartitionValue = repartitionValue;
        return this;
    }

    /**
     * Setting sort columns
     * @param sortColumns
     * @return
     */
    public UpsertBuilder sortColumns(List<String> sortColumns) {
        this.sortColumns = sortColumns;
        return this;
    }

    /**
     * This method will return the corresponding Upsert object.
     * @return Transformer
     */
    public DeltaLakeUpsertWriter build(){
        return new DeltaLakeUpsertWriter(
                new UpsertConfig(sparkSession, ds, columnsToInsert, columnsToUpsert, joinOnColumns,
                        options, deltaLocation, partitionColumns, repartitionValue, sortColumns));

    }
}
