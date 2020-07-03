package com.acp.azureSqlDataBase;

import com.acp.Loader;
import com.acp.LoaderBuilder;
import com.acp.enums.SqlLoaderType;
import com.acp.models.ConnectionConfig;
import com.microsoft.azure.sqldb.spark.connect.DataFrameFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class SqlDbLoaderTest {

    private ConnectionConfig connectionConfig;
    private Dataset dataset;

    @Before
    public void setUp() {
        connectionConfig = ConnectionConfig
                .builder()
                .url("localhost:1433")
                .database("test_db")
                .username("test_user")
                .password("test_password")
                .build();
        dataset = PowerMockito.mock(Dataset.class);
        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("dateOfBirth", DataTypes.TimestampType, false, Metadata.empty())
        });
        when(dataset.schema()).thenReturn(schema);
    }

    @Test
    public void testLoad() throws Exception {
        DataFrameFunctions<Dataset<Row>> mockDataFrameFunctions = mock(DataFrameFunctions.class);
        PowerMockito.whenNew(DataFrameFunctions.class).withAnyArguments().thenReturn(mockDataFrameFunctions);
        doNothing().when(mockDataFrameFunctions).bulkCopyToSqlDB(anyObject(), anyObject(), anyBoolean());

        Map<String, String> options = new HashMap<>();
        options.put("bulkCopyBatchSize", "8000");
        options.put("bulkCopyTableLock", "true");
        options.put("bulkCopyTimeout", "6000");

        Loader loader = LoaderBuilder
                .builder()
                .sqlLoaderType(SqlLoaderType.AZURE_SQL_DB)
                .connectionConfig(connectionConfig)
                .options(options)
                .createTable(false)
                .table("test_tableName")
                .build();

        loader.load(dataset);

        //TODO: This has been commented because jacaco is not capturing code coverage
        // with power mockito when annotated with
        // @PrepareForTest({DataFrameFunctions.class, DbLoader.class})
        /*Mockito.verify(mockDataFrameFunctions, Mockito.times(1)).
                bulkCopyToSqlDB(anyObject(), anyObject(), anyBoolean());*/
    }

    @Test
    public void testGetJavaSqlDataTypeCode(){
        SqlDbLoader sqlDbLoader = new SqlDbLoader();
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("decimal"), Types.DECIMAL);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("string"), Types.VARCHAR);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("integer"), Types.INTEGER);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("long"), Types.BIGINT);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("date"), Types.DATE);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("timestamp"), Types.TIMESTAMP);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("double"), Types.DOUBLE);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("float"), Types.FLOAT);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("short"), Types.SMALLINT);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("decimal"), Types.DECIMAL);
        assertEquals(sqlDbLoader.getJavaSqlDataTypeCode("null"), Types.NULL);

    }
}
