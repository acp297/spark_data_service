package com.acp.jobs.job;

import com.acp.ReaderBuilder;
import com.acp.config.JobConfig;
import com.acp.enums.FileTypeEnum;
import com.acp.enums.SqlLoaderType;
import com.acp.helper.WaterMarkHelperService;
import com.acp.jobs.enums.JobType;
import com.acp.models.BaseConnectionConfig;
import com.acp.models.ConnectionConfig;
import com.acp.readers.Reader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.max;

/**
 * This class fetch req config. Identifies all new records to be inserted and updated
 * based on watermark cols.
 * Loads them to Data warehouse.
 *
 * @author Anand Prakash
 */
@Slf4j
public abstract class AbstractSqlLoader {
    private SparkSession sparkSession;
    private FileTypeEnum fileType;
    private String sourcePath;
    private String schemaName;
    private String tableName;
    public String[] finalColumnsList;
    public String finalTableName;
    public String tempTableName;
    private final String DOT = ".";
    private final String UNDER_SCORE = "_";
    public final String TEMP = "temp";
    public final String TEMP_DIR = "tempDir";
    public final String LOGIN_TIME_OUT = "30";
    private final String WATER_MARK_TABLE = "water_mark";
    private final String CREATED_AT = "_created_at";
    private final String UPDATED_AT = "_updated_at";
    private final String SUB_PROTOCOL = "jdbc:sqlserver";
    private final String PORT = "1433";
    public final String TRUE = "true";
    public final String FALSE = "false";
    private WaterMarkHelperService waterMarkHelperService;
    private Dataset<Row> allNewRecords;
    public SqlLoaderType sqlLoaderType;
    public ConnectionConfig.ConnectionConfigBuilder connectionConfigBuilder;

    /**
     * Initialize job config.
     * @param jobConfig
     */
    public void initializeLoaderConfig(JobConfig jobConfig){
        sparkSession = jobConfig.getSparkSession();
        fileType = FileTypeEnum.valueOf(jobConfig.getSourceFileType().toUpperCase());
        sourcePath = jobConfig.getSourcePaths()[0];
        schemaName = jobConfig.getSchemaName();
        tableName = jobConfig.getTableName();
        finalColumnsList = jobConfig.getFinalCols();
        finalTableName = schemaName.concat(DOT).concat(tableName);
        tempTableName = finalTableName.concat(UNDER_SCORE).concat(TEMP);
        BaseConnectionConfig connectionConfig = jobConfig
                .getConfiguration()
                .getSqlConnectionConfig();

        String url = connectionConfig.getUrl();
        String database = connectionConfig.getDatabase();
        String username = connectionConfig.getUsername();
        String password = connectionConfig.getPassword();
        String jdbcUrl = buildJdbcUrl(url);

        connectionConfigBuilder = ConnectionConfig
                .builder()
                .url(url)
                .jdbcUrl(jdbcUrl)
                .database(database)
                .username(username)
                .password(password);
        waterMarkHelperService = getWaterMarkHelperService();

        if (JobType.LOAD_TO_MS_SQL_DW.name().equalsIgnoreCase(jobConfig.getJobType())){
            sqlLoaderType = SqlLoaderType.AZURE_SQL_DW;
        } else {
            sqlLoaderType = SqlLoaderType.AZURE_SQL_DB;
        }
    }

    /**
     * Read source and identifies new records based on prev runtime.
     * @return
     * @throws SQLException
     */
    public Dataset<Row> loadNewRecords() throws SQLException {
        Dataset<Row> allRecords = readSource();
        log.info("Source schema: " + allRecords.schema().treeString());

        Long prevRunTime = fetchPrevRunTime();
        if (prevRunTime.equals(0L)) {
            log.info("Running Full Load.......");
        }
        allNewRecords = getAllNewRecords(allRecords, prevRunTime);
        return allNewRecords;
    }

    /**
     * Util function to read source.
     * @return
     */
    private Dataset<Row> readSource(){
        Reader reader = ReaderBuilder.builder()
                .sparkSession(sparkSession)
                .readerType(fileType)
                .path(sourcePath)
                .build();
        log.info("Reading delta...");

        return reader.read();
    }

    /**
     * Fetch successful previous run time from watermark column.
     * @return
     * @throws SQLException
     */
    public Long fetchPrevRunTime() throws SQLException {
        return waterMarkHelperService.fetchPrevRunTime(tableName);
    }

    /**
     * Update watermark column for table name with new updated at.
     */
    public void updateWaterMark(){
        String updatedAt = getUpdatedAt(allNewRecords);
        waterMarkHelperService.updateWaterMarkCol(tableName, Long.valueOf(updatedAt));
        waterMarkHelperService.closeConnection();
    }

    /**
     * Get water mark helper service.
     * @return
     */
    WaterMarkHelperService getWaterMarkHelperService() {
        return new WaterMarkHelperService(connectionConfigBuilder.build(),
                schemaName, WATER_MARK_TABLE, tableName);
    }

    /**
     * Filter all new inserted/updated records based on prevRunTime
     *
     * @param ds
     * @param prevRunTime
     * @return
     */
    public Dataset<Row> getAllNewRecords(Dataset<Row> ds, Long prevRunTime) {
        log.info("Getting newly added records...");
        return ds.filter(ds.col(UPDATED_AT).$greater(prevRunTime));
    }

    /**
     * Get max column value of _updated_at in the dataset.
     *
     * @param ds
     * @return
     */
    public String getUpdatedAt(Dataset<Row> ds) {
        log.info("Getting new prevRunTime");
        return ds.agg(max(ds.col(UPDATED_AT))).first().get(0).toString();
    }

    /**
     * Identifies records which has to be inserted.
     *
     * @param ds
     * @return
     */
    public Dataset<Row> getRecordsToInsert(Dataset<Row> ds) {
        log.info("Identifying records to be inserted into data warehouse");
        return ds.filter(ds.col(CREATED_AT).equalTo(ds.col(UPDATED_AT)));
    }

    /**
     * Identifies records which has to be updated.
     *
     * @param ds
     * @return
     */
    public Dataset<Row> getRecordsToUpdate(Dataset<Row> ds) {
        log.info("Identifying records to be updated into data warehouse");
        return ds.filter(ds.col(CREATED_AT).notEqual(ds.col(UPDATED_AT)));
    }

    /**
     * Selects all finalColumns provided as input
     *
     * @param ds
     * @param finalColumns
     * @return
     */
    public Dataset<Row> getFinalSelectedColumns(Dataset<Row> ds, String[] finalColumns)
            throws SQLException {

        if (finalColumns == null || finalColumns.length == 0) {
            List<String> columns = Arrays.asList(ds.columns());
            if (columns.contains(CREATED_AT) && columns.contains(UPDATED_AT)){
                return ds.drop(CREATED_AT, UPDATED_AT);
            }
            return ds;
        }
        List<String> finalArrangedColumns = arrangeFinalColumnsInExistingTableOrder(finalColumns);
        return ds.selectExpr(finalArrangedColumns.toArray(new String[0]));
    }

    /**
     * Arrange final column list based on existing order of columns in table.
     * @param finalColumns
     * @return
     * @throws SQLException
     */
    private List<String> arrangeFinalColumnsInExistingTableOrder(String[] finalColumns)
            throws SQLException {
        if(waterMarkHelperService.isTableExists()){
            List<String> sqlOrderColumnNames = waterMarkHelperService.getColumnNames();
            return sortListWrtGivenList(sqlOrderColumnNames,
                            Arrays.asList(finalColumns));
        }
        return Arrays.asList(finalColumns);
    }

    /**
     * Sort a list based on corresponding to provided list order.
     * @param existingColumns
     * @param newColumns
     * @return
     */
    List<String> sortListWrtGivenList(List<String> existingColumns,
                                              List<String> newColumns) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0 ; i < existingColumns.size() ; i++) {
            map.put(existingColumns.get(i), i);
        }
        String[] arrangedColumns = new String[existingColumns.size()];
        for (String newColumn : newColumns) {
            arrangedColumns[map.get(newColumn)] = newColumn;
        }
        return Arrays.asList(arrangedColumns);
    }

    private String buildJdbcUrl(String url){
        return SUB_PROTOCOL + File.pathSeparator + File.separator +
                File.separator + url + File.pathSeparator + PORT;
    }
}
