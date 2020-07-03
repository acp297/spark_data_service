package com.acp.helper;

import com.acp.models.ConnectionConfig;
import lombok.extern.log4j.Log4j;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 *
 * A class to manage watermark table which tracks Last updated time of a SQL DW
 * @author Anand Prakash
 */

@Log4j
public class WaterMarkHelperService implements Serializable {
    private ConnectionConfig connectionConfig;
    private String waterMarkTableName;
    private String schemaName;
    private String tableName;
    private Connection connection;

    public WaterMarkHelperService(ConnectionConfig connectionConfig,
                                  String schemaName, String waterMark,
                                  String tableName) {
        this.connectionConfig = connectionConfig;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.waterMarkTableName = schemaName + "." + waterMark;
    }

    /**
     * Updates watermark table after every run
     * Creates a new entry if it does not already exist
     *
     * @param key
     * @param sparkPrevRunTime
     */


    public void updateWaterMarkCol(String key, Long sparkPrevRunTime) {
        try {
            if (prevRunTimeForTableExists(key)){
                updatePrevRuntime(key, sparkPrevRunTime);}
            else{
                insertPrevRunTime(key, sparkPrevRunTime);}
        } catch (SQLException e) {
            throw new RuntimeException("Exception occurred  " + e.getLocalizedMessage());
        }
    }

    /**
     * Inserts previous runtime into watermark table for given table
     *
     * @param key
     * @param sparkPrevRunTime
     * @throws SQLException
     */

    private void insertPrevRunTime(String key,
                                   long sparkPrevRunTime)
            throws SQLException {
        log.info("Inserting sparkPrevRunTime column...");
        String insertInWaterMarkTableQuery =
                "INSERT INTO " + waterMarkTableName + "(tableName, sparkPrevRunTime) " +
                        "VALUES" + "(?,?)";

        try (PreparedStatement insertPreparedStatement =
                     getConnection().prepareStatement(insertInWaterMarkTableQuery)) {

            insertPreparedStatement.setString(1, key);
            insertPreparedStatement.setLong(2, sparkPrevRunTime);

            int recordsUpdated = insertPreparedStatement.executeUpdate();
            if (recordsUpdated == 1) {
                log.info("sparkPrevRunTime column is inserted in " +
                        "watermark table for tableName:" + key);
            } else {
                log.error("Exception occurred during inserting watermark col");
                throw new RuntimeException("Exception occurred during updating watermark col");
            }
        }
    }

    /**
     * Updates previous runtime into watermark table for given table
     *
     * @param key
     * @param sparkPrevRunTime
     * @throws SQLException
     */

    private void updatePrevRuntime(String key, long sparkPrevRunTime) throws SQLException {
        log.info("Updating sparkPrevRunTime column...");
        String UpdateWaterMarkTableQuery =
                "UPDATE " + waterMarkTableName + " SET sparkPrevRunTime = ? " +
                        " where tableName = ?";

        try (PreparedStatement insertPreparedStatement =
                     getConnection().prepareStatement(UpdateWaterMarkTableQuery)) {

            insertPreparedStatement.setLong(1, sparkPrevRunTime);
            insertPreparedStatement.setString(2, key);

            int recordsUpdated = insertPreparedStatement.executeUpdate();
            if (recordsUpdated == 1) {
                log.info("sparkPrevRunTime column updated for table  " + key);
            } else {
                log.error("Exception occurred during updating sparkPrevRunTime column");
                throw new RuntimeException("Exception occurred during " +
                        "updating sparkPrevRunTime column");
            }
        }
    }

    /**
     * Fetches previous run time for a given table
     *
     * @param key
     * @return
     * @throws SQLException
     */

    public Long fetchPrevRunTime(String key) throws SQLException {
        ResultSet rs = getResultSet(key);
        if (rs.next()) {
            String storedProcPrevRunTime = rs.getString("storedProcPrevRunTime");
            if (storedProcPrevRunTime == null) {
                return 0L;
            } else {
                return Long.parseLong(storedProcPrevRunTime);
            }
        } else {
            return 0L;
        }
    }

    /**
     * Checks if previous Runtime exists for given table
     *
     * @param key
     * @return
     * @throws SQLException
     */

    private boolean prevRunTimeForTableExists(String key) throws SQLException {
        log.info("Checking if sparkPrevRunTime exists for tableName: " + key);
        return getResultSet(key).next();
    }

    /**
     * Gets sparkPrevRunTime and stored procedure runtime for gievn table
     *
     * @param key
     * @return
     * @throws SQLException
     */

    private ResultSet getResultSet(String key) throws SQLException {
        log.info("Fetching sparkPrevRunTime for tableName: " + key);
        String fetchRecordQuery =
                "SELECT sparkPrevRunTime , storedProcPrevRunTime  FROM " + waterMarkTableName +
                        " WHERE tableName = ?";
        PreparedStatement selectPreparedStatement =
                getConnection().prepareStatement(fetchRecordQuery);
        selectPreparedStatement.setString(1, key);

        return selectPreparedStatement.executeQuery();
    }

    public Connection getConnection() {
        if (!Optional.ofNullable(connection).isPresent()) {
            connection = getDBConnection();
            return connection;
        }
        return connection;
    }

    /**
     * Establish the JDBC connection.
     * @return Connection
     */
    public Connection getDBConnection() {
        log.info("Establishing connection with Data Warehouse...");
        try {
            connection = DriverManager.getConnection(connectionConfig.getJdbcUrl()
                    + ";databaseName=" + connectionConfig.getDatabase() + ";user="
                    + connectionConfig.getUsername() +
                    ";password=" + connectionConfig.getPassword());
            log.info("Connection successfully established with Data Warehouse...");
        } catch (SQLException e) {
            log.error("Error while creating db connection " + e.getMessage());
            throw new RuntimeException("Error while creating db connection " + e.getMessage());
        }
        return connection;
    }

    /**
     * Fetch all the columns present in the table.
     * @return
     * @throws SQLException
     */
    public List<String> getColumnNames() throws SQLException {
        ResultSet resultSet = getConnection()
                .getMetaData().getColumns(null, schemaName, tableName, null);

        List<String> columns = new ArrayList<>();
        while (resultSet.next()) {
            columns.add(resultSet.getString("COLUMN_NAME"));
        }
        return columns;
    }

    /**
     * Check if table exists.
     * @return
     * @throws SQLException
     */
    public boolean isTableExists() throws SQLException {
        ResultSet tables = getConnection()
                .getMetaData().getTables(null, schemaName, tableName, null);
        return tables.next();
    }

    /**
     * Closes the JDBC connection.
     */
    public void closeConnection() {
        log.info("Closing connection with Data Warehouse...");
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Error while closing connection " + e.getMessage());
            throw new RuntimeException("Error while closing connection " + e.getMessage());
        }
    }


}
