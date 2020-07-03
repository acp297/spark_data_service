package com.acp.sqlJdbc;

import com.acp.models.BaseConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.util.LongAccumulator;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A class to update records in SQL DW using sql jdbc driver
 * @author Anand Prakash
 */
@Slf4j
public class SqlJdbcUpdate implements Serializable {
    private SparkSession sparkSession;
    private BaseConnectionConfig connectionConfig;

    public SqlJdbcUpdate(SparkSession sparkSession, BaseConnectionConfig connectionConfig) {
        this.sparkSession = sparkSession;
        this.connectionConfig = connectionConfig;
    }

    /**
     * Updates given set of records into SQL DW
     *
     * @param recordsToUpdate
     * @param tableName
     * @param primaryKeys
     */
    public void update(Dataset<Row> recordsToUpdate, String tableName, List<String> primaryKeys) {

        Map<String, String> nonPrimaryFieldToDataTypeMapping = new LinkedHashMap<>();
        Map<String, String> primaryFieldToDataTypeMapping = new LinkedHashMap<>();

        loadFieldToDataMapping(recordsToUpdate, primaryKeys,
                nonPrimaryFieldToDataTypeMapping, primaryFieldToDataTypeMapping);

        String query = getQuery(nonPrimaryFieldToDataTypeMapping.keySet(),
                tableName, primaryFieldToDataTypeMapping.keySet());

        SparkContext sparkContext = sparkSession.sparkContext();
        Broadcast<String> queryBroadCast =
                sparkContext.broadcast(query, classTag(String.class));
        Broadcast<Map> primaryFieldsBroadCast =
                sparkContext.broadcast(primaryFieldToDataTypeMapping, classTag(Map.class));
        Broadcast<Map> nonPrimaryFieldsBroadCast =
                sparkContext.broadcast(nonPrimaryFieldToDataTypeMapping, classTag(Map.class));

        LongAccumulator totalUpdatedRecordsAccumulator = sparkContext.longAccumulator();
        recordsToUpdate.foreachPartition((ForeachPartitionFunction<Row>) partition -> {
            final int BATCH_SIZE = 5000;

            String sqlDbUrl = connectionConfig.getUrl();
            String database = connectionConfig.getDatabase();
            String user  = connectionConfig.getUsername();
            String pass = connectionConfig.getPassword();

            log.info("Establishing connection with data warehouse...");
            Connection connection = DriverManager.getConnection(sqlDbUrl
                    + ";databaseName=" + database + ";user=" + user + ";password=" + pass);

            PreparedStatement preparedStatement =
                    connection.prepareStatement(queryBroadCast.value());
            int count = 0;
            while (partition.hasNext()) {
                Row row = partition.next();

                prepareStatement(preparedStatement, row,
                        nonPrimaryFieldsBroadCast.value(), primaryFieldsBroadCast.value());
                preparedStatement.addBatch();
                count++;

                if (count % BATCH_SIZE == 0) {
                    int[] updatedRecordsCount = preparedStatement.executeBatch();
                    log.info("Records got updated in batch: " + updatedRecordsCount.length);
                    preparedStatement.clearBatch();
                    totalUpdatedRecordsAccumulator.add(count);
                    count = 0;
                }
            }
            if (count != 0) {
                int[] updatedRecords = preparedStatement.executeBatch();
                log.info("Records got updated in batch: " + updatedRecords.length);
                preparedStatement.clearBatch();
                totalUpdatedRecordsAccumulator.add(count);
            }

            connection.commit();
            preparedStatement.close();
            connection.close();
        });
        log.info("Total records got updated : " + totalUpdatedRecordsAccumulator.value());
    }

    private void loadFieldToDataMapping(Dataset<Row> recordsToUpdate, List<String> primaryKeys,
                                       Map<String, String> nonPrimaryFieldToDataTypeMapping,
                                       Map<String,
                                       String> primaryFieldToDataTypeMapping) {
        for (StructField structField : recordsToUpdate.schema().fields()) {
            String fieldName = structField.name();
            String type = structField.dataType().typeName();

            if (primaryKeys.contains(fieldName)) {
                primaryFieldToDataTypeMapping.put(fieldName, type);
            } else {
                nonPrimaryFieldToDataTypeMapping.put(fieldName, type);
            }
        }
    }

    private void prepareStatement(PreparedStatement preparedStatement,
                                  Row row, Map<String, String> nonPrimaryFields,
                                  Map<String, String> primaryFields) throws SQLException {

        int colIndex = 1;
        for (Map.Entry<String, String> nonPrimaryField : nonPrimaryFields.entrySet()) {
            String colName = nonPrimaryField.getKey();
            String type = nonPrimaryField.getValue();
            Object newFieldValue = row.getAs(colName);

            prepareStatement(preparedStatement, newFieldValue, colIndex++, type);
        }

        for (Map.Entry<String, String> primaryField : primaryFields.entrySet()) {
            String colName = primaryField.getKey();
            String type = primaryField.getValue();
            Object newFieldValue = row.getAs(colName);

            prepareStatement(preparedStatement, newFieldValue, colIndex++, type);
        }
    }

    private void prepareStatement(PreparedStatement preparedStatement,
                                  Object newFieldValue, int colIndex, String dataType)
                                  throws SQLException {

        if (Pattern.matches("decimal.*", dataType)) {
            dataType = "decimal";
        }

        switch (dataType) {
            case "string":
                if (newFieldValue != null) {
                    preparedStatement.setString(colIndex, (String) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.NULL);
                }
                break;

            case "integer":
                if (newFieldValue != null) {
                    preparedStatement.setInt(colIndex, (Integer) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.INTEGER);
                }
                break;

            case "long":
                if (newFieldValue != null) {
                    preparedStatement.setLong(colIndex, (Long) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.NULL);
                }
                break;

            case "date":
                if (newFieldValue != null) {
                    preparedStatement.setDate(colIndex, (Date) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.DATE);
                }
                break;

            case "timestamp":
                if (newFieldValue != null) {
                    preparedStatement.setTimestamp(colIndex, (Timestamp) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.TIMESTAMP);
                }
                break;

            case "boolean":
                if (newFieldValue != null) {
                    preparedStatement.setBoolean(colIndex, (Boolean) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.BOOLEAN);
                }
                break;

            case "double":
                if (newFieldValue != null) {
                    preparedStatement.setDouble(colIndex, (Double) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.DOUBLE);
                }
                break;

            case "decimal":
                if (newFieldValue != null) {
                    preparedStatement.setBigDecimal(colIndex, (BigDecimal) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.DECIMAL);
                }
                break;

            case "float":
                if (newFieldValue != null) {
                    preparedStatement.setFloat(colIndex, (Float) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.FLOAT);
                }
                break;

            case "short":
                if (newFieldValue != null) {
                    preparedStatement.setShort(colIndex, (Short) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.NULL);
                }
                break;

            case "byte":
                if (newFieldValue != null) {
                    preparedStatement.setByte(colIndex, (Byte) newFieldValue);
                } else {
                    preparedStatement.setNull(colIndex, Types.NULL);
                }
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + dataType);
        }
    }


    /**
     *
     * Generates UPDATE query to perform updation of records in DW
     *
     * @param fieldList
     * @param tableName
     * @param primaryKeys
     * @return
     */
    private String getQuery(Set<String> fieldList,
                            String tableName, Set<String> primaryKeys) {
        String UPDATE = "UPDATE";
        String SET = "SET";
        String WHERE = "WHERE";
        String COMMA = ",";
        String SPACE = " ";
        String EQUALTO = "=";
        String QUESTION = "?";
        String AND = "AND";

        String query = UPDATE.concat(SPACE).concat(tableName).concat(SPACE)
                .concat(SET).concat(SPACE);
        Iterator<String> fields = fieldList.iterator();

        while (fields.hasNext()) {
            String field = fields.next();
            query = query.concat(field).concat(SPACE).concat(EQUALTO)
                    .concat(SPACE).concat(QUESTION).concat(SPACE);
            if (fields.hasNext()) {
                query = query.concat(COMMA).concat(SPACE);
            }
        }

        if (!primaryKeys.isEmpty()) {
            Iterator<String> pks = primaryKeys.iterator();
            query = query.concat(WHERE).concat(SPACE);

            while (pks.hasNext()) {
                query = query.concat(pks.next())
                        .concat(SPACE).concat(EQUALTO).concat(SPACE).concat(QUESTION);

                if (pks.hasNext()) {
                    query = query.concat(SPACE).concat(AND).concat(SPACE);
                }
            }
        }
        return query;
    }

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }
}
