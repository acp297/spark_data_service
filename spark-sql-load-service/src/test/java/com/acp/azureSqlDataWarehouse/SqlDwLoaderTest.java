package com.acp.azureSqlDataWarehouse;

import com.acp.Loader;
import com.acp.LoaderBuilder;
import com.acp.enums.SqlLoaderType;
import com.acp.models.ConnectionConfig;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore("javax.security.*")
@PrepareForTest(DataFrameWriter.class)
@RunWith(PowerMockRunner.class)
public class SqlDwLoaderTest {
    private ConnectionConfig connectionConfig;
    private Dataset dataset;
    private DataFrameWriter dataFrameWriter;

    @Before
    public void setUp() {

        dataset = PowerMockito.mock(Dataset.class);
        dataFrameWriter = PowerMockito.mock(DataFrameWriter.class);
        connectionConfig = ConnectionConfig
                .builder()
                .url("jdbc:sqlserver://localhost:1433")
                .database("test_db")
                .username("test_user")
                .password("test_password")
                .format("com.databricks.spark.sqldw")
                .encrypt("true")
                .trustServerCertificate("false")
                .hostNameInCertificate("*.database.windows.net")
                .loginTimeout("30")
                .build();
    }

    @Test
    public void testLoad() {
        Mockito.when(dataset.write()).thenReturn(dataFrameWriter);
        Mockito.doNothing().when(dataFrameWriter).save();

        Loader loader = LoaderBuilder
                .builder()
                .sqlLoaderType(SqlLoaderType.AZURE_SQL_DW)
                .connectionConfig(connectionConfig)
                .table("test_tableName")
                .mode(SaveMode.Append)
                .options(null)
                .build();

        loader.load(dataset);
        Mockito.verify(dataFrameWriter, Mockito.times(1)).save();

    }
}
