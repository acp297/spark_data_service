package com.acp.jobs.job;

import com.acp.Loader;
import com.acp.LoaderBuilder;
import com.acp.config.JobConfig;
import com.acp.enums.SqlLoaderType;
import com.acp.helper.Util;
import com.acp.models.BaseConnectionConfig;
import com.acp.sqlJdbc.SqlJdbcUpdate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.acp.constants.AzureConstants.*;

/**
 * This class fetch req config. Identifies all new records to be inserted and updated
 * based on watermark cols.
 * Loads them to Data warehouse.
 *
 * @author Anand Prakash
 */
@Slf4j
public class LoadToSql extends AbstractSqlLoader implements Job {
    private JobConfig jobConfig;

    /**
     * Initialize job config.
     * @param jobConfig
     */
    @Override
    public void init(JobConfig jobConfig) {
        this.jobConfig = jobConfig;
        initializeLoaderConfig(jobConfig);
    }

    /**
     * Parent method which initiates loading.
     * @throws SQLException
     */
    @Override
    public void run() throws SQLException {
        prepareAndLoad();
    }

    /**
     * Identifies final records to be inserted and updated.
     * Loads them to Data warehouse or Databased on Job type.
     *
     * @throws SQLException
     */
    public void prepareAndLoad() throws SQLException {
        log.info("Loading data from delta lake to " + sqlLoaderType.getName());
        Dataset<Row> allNewRecords = loadNewRecords();
        if (allNewRecords.isEmpty()) {
            log.info("No new records to be inserted or updated");
            return;
        }
        Dataset<Row> recordsToInsert = getRecordsToInsert(allNewRecords);
        Dataset<Row> recordsToUpdate = getRecordsToUpdate(allNewRecords);

        if (!recordsToInsert.isEmpty()){
            Dataset<Row> finalRecordsToInsert =
                    getFinalSelectedColumns(recordsToInsert, finalColumnsList);

            log.info("Dataset schema which is to be loaded to " + sqlLoaderType.getName() +
                    " :" + finalRecordsToInsert.schema().treeString());
            log.info("Records getting inserted into table : " + finalTableName);
            load(finalRecordsToInsert, finalTableName, SaveMode.Append, false);
        }

        if (!recordsToUpdate.isEmpty()){
            Dataset<Row> finalRecordsToUpdate =
                    getFinalSelectedColumns(recordsToUpdate, finalColumnsList);
            log.info("Records getting inserted into table : " + tempTableName);
            load(finalRecordsToUpdate, tempTableName, SaveMode.Overwrite, true);
        }
        updateWaterMark();
        // If we want to update the main table using JDBC connection.
        // updateToDw(sparkSession, connectionConfig,
        // finalRecordsToUpdate, finalTableName, primaryKey);
    }

    /**
     * A function to insert a dataset to given table in DW
     *  @param ds
     * @param tableName
     * @param saveMode
     * @param createTable
     */

    public void load(Dataset<Row> ds, String tableName, SaveMode saveMode, boolean createTable) {
        log.info("Inserting data to " + sqlLoaderType.getName());

        Map<String, String> options;
        if (sqlLoaderType.getName().equalsIgnoreCase(SqlLoaderType.AZURE_SQL_DW.getName())){
            String tempPath = jobConfig.getSchemaName() +
                    File.separator + TEMP +  File.separator + tableName;
            options = new HashMap<>();
            options.put(USE_AZURE_MSI, TRUE);
            options.put(TEMP_DIR, Util.generateAbfssPath(tempPath,
                    jobConfig.getConfiguration().getAdlsConfig().getAccountName()));
            connectionConfigBuilder
                    .format(DW_FORMAT)
                    .hostNameInCertificate(DW_HOST_CERTIFICATE)
                    .encrypt(TRUE)
                    .trustServerCertificate(FALSE)
                    .loginTimeout(LOGIN_TIME_OUT);
        } else {
            options = jobConfig.getJobConf();
        }

        Loader loader = LoaderBuilder
                .builder()
                .sqlLoaderType(sqlLoaderType)
                .connectionConfig(connectionConfigBuilder.build())
                .table(tableName)
                .options(options)
                .mode(saveMode)
                .createTable(createTable)
                .build();

        loader.load(ds);
        log.info("Successfully inserted to " + sqlLoaderType.getName());
    }

    /**
     * A function to add updated records to given table in DW
     *
     * @param sparkSession
     * @param connectionConfig
     * @param ds
     * @param tableName
     * @param primaryKey
     */
    public void update(SparkSession sparkSession,
                           BaseConnectionConfig connectionConfig,
                           Dataset<Row> ds, String tableName,
                           String[] primaryKey) {
        log.info("Updating in data warehouse....");
        SqlJdbcUpdate sqlJdbcUpdate = new SqlJdbcUpdate(sparkSession, connectionConfig);
        sqlJdbcUpdate.update(ds, tableName, Arrays.asList(primaryKey));
        log.info("Successfully updated in data warehouse");
    }
}
