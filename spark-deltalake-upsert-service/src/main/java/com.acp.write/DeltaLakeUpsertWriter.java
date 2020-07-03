package com.acp.write;

import com.acp.write.config.UpsertConfig;
import io.delta.tables.DeltaTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.stream.Collectors;

import static com.acp.constants.SparkOptions.MERGE_SCHEMA;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;


/**
 * This class Deals with Upsert transformation.
 * @author Anand Prakash
 */
@Slf4j
public class DeltaLakeUpsertWriter {

    private UpsertConfig upsertConfig;
    private final String CREATED_AT = "_created_at";
    private final String UPDATED_AT = "_updated_at";
    private final String SOURCE = "source";
    private final String TARGET = "target";
    private final String DOT = ".";
    private final String DELTA = "delta";
    private final String TRUE = "true";
    private final String DYNAMIC_PARTITION_PRUNNING =
            "spark.databricks.optimizer.dynamicPartitionPruning";
    private Map<String, String> columnsToInsert;
    private Map<String, String> columnsToUpsert;


    public DeltaLakeUpsertWriter(UpsertConfig upsertConfig) {
        this.upsertConfig = upsertConfig;
    }

    public DeltaLakeUpsertWriter() {
    }


    /**
     * Upsert the data to the provided delta location.
     * @return
     */
    public void upsert() {
        SparkSession spark = upsertConfig.getSparkSession();
        String deltaLocation = upsertConfig.getDeltaLocation();
        Dataset<Row> sourceDs = upsertConfig.getDs();
        columnsToInsert = upsertConfig.getColumnsToInsert();
        columnsToUpsert = upsertConfig.getColumnsToUpsert();
        Map<String, String> columnMapping = upsertConfig.getJoinOnColumns();
        Dataset<Row> sourceWithWaterMarkCol = addWatermarkColumn(sourceDs);
        Dataset<Row> finalSourceDs = null;
        DeltaTable finalDeltaTable;
        spark.conf().set(DYNAMIC_PARTITION_PRUNNING, "true");

        if (!isDeltaTable(spark, deltaLocation)) {
            save(deltaLocation, sourceWithWaterMarkCol);
        } else {
            finalDeltaTable = getOrCreateDeltaTable(spark, deltaLocation);
            Collection additionalColumnsInSource =
                    getColumnDiff(sourceWithWaterMarkCol, finalDeltaTable.toDF());
            Collection additionalColumnsInTarget =
                    getColumnDiff(finalDeltaTable.toDF(), sourceWithWaterMarkCol);

            if (!additionalColumnsInSource.isEmpty()) {
                Dataset<Row> emptyDataSetWithSourceSchema =
                        getEmptyDataset(spark, sourceWithWaterMarkCol);
                save(deltaLocation, emptyDataSetWithSourceSchema);
                finalDeltaTable = getOrCreateDeltaTable(spark, deltaLocation);
                finalSourceDs = sourceWithWaterMarkCol;
            } else if (!additionalColumnsInTarget.isEmpty()) {
                Map<String, DataType> columnToDataTypeMapping = new HashMap<>();
                loadFieldToDataMapping(finalDeltaTable.toDF(), columnToDataTypeMapping);
                for (Object col : additionalColumnsInTarget.toArray()) {
                    DataType dataType = columnToDataTypeMapping.get(col.toString());
                    sourceWithWaterMarkCol = sourceWithWaterMarkCol
                            .withColumn(col.toString(), lit(null).cast(dataType));
                }
                finalSourceDs = sourceWithWaterMarkCol;
            } else {
                finalSourceDs = sourceWithWaterMarkCol;
            }
            addRuleMapping(finalSourceDs);
            Dataset<Row> sortedDataset = sort(finalSourceDs, upsertConfig.getSortColumns());
            merge(sortedDataset, finalDeltaTable, columnsToInsert,
                    columnsToUpsert, columnMapping);
        }
    }

    /**
     * Saving dataset in delta location
     * @param deltaLocation
     * @param dataset
     */
    private void save(String deltaLocation, Dataset<Row> dataset) {
        List<String> partitionColumns = upsertConfig.getPartitionColumns();
        log.info("Partition columns {}", partitionColumns);
        Dataset<Row> sortedDataset = sort(dataset, upsertConfig.getSortColumns());
        Dataset<Row> repartitionDatsset = repartition(sortedDataset, partitionColumns,
                upsertConfig.getRepartitionValue());
        DataFrameWriter<Row> dataFrameWriter = repartitionDatsset.write();
        setBaseConfig(dataFrameWriter);
        if (CollectionUtils.isNotEmpty(partitionColumns)) {
            log.debug("Setting partition columns {}", partitionColumns);
            dataFrameWriter.partitionBy(JavaConversions.asScalaBuffer(
                    upsertConfig.getPartitionColumns()).seq());
        }
        log.info("Saving data to delta location {}", deltaLocation);
        dataFrameWriter.save(deltaLocation);
    }

    /**
     * Repartitioning by partition columns if repartitionValue is specified
     * @param dataset
     * @param partitionColumns
     * @param repartitionValue
     */
    private Dataset<Row> repartition(Dataset<Row> dataset, List<String> partitionColumns,
                                              int repartitionValue) {
        if (repartitionValue > 0) {
            if (CollectionUtils.isNotEmpty(partitionColumns)) {
                log.info("Repartitioning by partition columns {}", partitionColumns);
                List<Column> cols = partitionColumns.stream().map(columnName ->
                        col(columnName)).collect(Collectors.toList());
                return dataset.repartition(repartitionValue,
                        JavaConversions.asScalaBuffer(cols).seq());
            } else {
                log.info("Repartitioning by repartition value {}", repartitionValue);
                return dataset.repartition(repartitionValue);
            }
        } else {
            return dataset;
        }
    }

    /**
     * Sorting dataset by column
     * @param dataset
     * @param sortColumns
     * @return
     */
    private Dataset<Row> sort(Dataset<Row> dataset, List<String> sortColumns) {
        if (CollectionUtils.isNotEmpty(sortColumns)) {
            log.info("Sorting columns {}", sortColumns);
            List<Column> cols = sortColumns.stream().map(columnName ->
                    col(columnName)).collect(Collectors.toList());
            return dataset.sort(JavaConversions.asScalaBuffer(cols).seq());
        } else {
            return dataset;
        }
    }

    /**
     * Loads column name to data type in a HashMap.
     * @param ds
     * @param columnToDataTypeMapping
     */
    private void loadFieldToDataMapping(Dataset<Row> ds,
                                        Map<String, DataType> columnToDataTypeMapping) {
        for (StructField structField : ds.schema().fields()) {
            String fieldName = structField.name();
            DataType type = structField.dataType();
            columnToDataTypeMapping.put(fieldName, type);
        }
    }

    /**
     * Returns column in left dataset less column in right dataset.
     * @param left
     * @param right
     * @return
     */
    private Collection getColumnDiff(Dataset<Row> left, Dataset<Row> right) {
        return CollectionUtils.subtract(Arrays.asList(left.schema().fieldNames()),
                Arrays.asList(right.schema().fieldNames()));
    }

    /**
     * Returns empty dataset with same provided dataset's schema.
     * @param spark
     * @param ds
     * @return
     */
    private Dataset<Row> getEmptyDataset(SparkSession spark, Dataset<Row> ds) {
        List<Row> rows = new ArrayList<>();
        return spark.createDataFrame(rows, ds.schema());
    }

    /**
     * Add _created_at and _updated_at columns with current time in milliseconds value.
     * @param ds
     * @return
     */
    private Dataset<Row> addWatermarkColumn(Dataset<Row> ds) {
        long currentTime = new Date().getTime();
        return ds.withColumn(CREATED_AT, lit(currentTime))
                .withColumn(UPDATED_AT, lit(currentTime));
    }

    private void addRuleMapping(Dataset<Row> ds){

        if (columnsToInsert == null){
            columnsToInsert = new HashMap<>();
        }

        if (columnsToUpsert == null){
            columnsToUpsert = new HashMap<>();
        }

        for (String field : ds.schema().fieldNames()) {
            String sourceValue = SOURCE.concat(DOT).concat(field);
            columnsToInsert.put(field, sourceValue);
            if (field.equals(CREATED_AT)) {
                columnsToUpsert.put(field, TARGET.concat(DOT).concat(field));
                continue;
            }
            columnsToUpsert.put(field, sourceValue);
        }
    }


    /**
     * Create a DeltaTable for the data at the given delta Location using the given SparkSession.
     * @param sparkSession
     * @param deltaLocation
     * @return
     */
    public DeltaTable getOrCreateDeltaTable(SparkSession sparkSession, String deltaLocation) {
        return DeltaTable.forPath(sparkSession, deltaLocation);
    }

    /**
     * Check if delta table exist at the given delta location.
     * @param sparkSession
     * @param deltaLocation
     * @return
     */
    public Boolean isDeltaTable(SparkSession sparkSession, String deltaLocation) {
        return DeltaTable.isDeltaTable(sparkSession, deltaLocation);
    }

    /**
     * Merge data from the source Dataset based on column mapping.
     * @param sourceDs
     * @param targetDeltaTable
     * @param columnsToInsert
     * @param columnsToUpsert
     * @param columnMapping
     */
    public void merge(Dataset<Row> sourceDs,
                      DeltaTable targetDeltaTable,
                      Map<String, String> columnsToInsert,
                      Map<String, String> columnsToUpsert,
                      Map<String, String> columnMapping) {
        merge(sourceDs, targetDeltaTable, columnsToInsert,
                columnsToUpsert, buildMergeCondition(columnMapping));
    }

    /**
     * Merge data from the source Dataset based on the given merge condition.
     * @param sourceDs
     * @param targetDeltaTable
     * @param columnsToInsert
     * @param columnsToUpsert
     * @param mergeCondition
     */
    public void merge(Dataset<Row> sourceDs,
                      DeltaTable targetDeltaTable,
                      Map<String, String> columnsToInsert,
                      Map<String, String> columnsToUpsert,
                      String mergeCondition) {

        targetDeltaTable.as(TARGET)
                .merge(sourceDs.as(SOURCE), mergeCondition)
                .whenMatched().updateExpr(columnsToUpsert)
                .whenNotMatched().insertExpr(columnsToInsert)
                .execute();
    }

    /**
     * Build merge condition expression using column mapping.
     * @param cols
     * @return
     */
    public String buildMergeCondition(Map<String, String> cols){

        final String equalsTo = "=";
        final String and = " and ";
        StringBuilder mergeCondition = new StringBuilder();

        Iterator<String> iterator = cols.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            mergeCondition.append(SOURCE).append(DOT).append(key).append(equalsTo)
                    .append(TARGET).append(DOT).append(cols.get(key));

            if (iterator.hasNext()){
                mergeCondition.append(and);
            }
        }
        return mergeCondition.toString();
    }


    /**
     * Set base config required for dataFrameWriter.
     * @param dataFrameWriter
     */
    private void setBaseConfig(DataFrameWriter<Row> dataFrameWriter) {
        dataFrameWriter.format(DELTA);
        dataFrameWriter.mode(SaveMode.Append);
        setOptions(dataFrameWriter, upsertConfig.getOptions());
    }

    /**
     * Adds output options for the underlying data source.
     * @param dataFrameWriter
     * @param options
     */
    private void setOptions(DataFrameWriter<Row> dataFrameWriter, Map<String, String> options) {
        if (options == null){
            options = new HashMap<>();
            options.put(MERGE_SCHEMA, TRUE);
        } else {
            options.putIfAbsent(MERGE_SCHEMA, TRUE);
        }
        dataFrameWriter.options(options);
    }
}