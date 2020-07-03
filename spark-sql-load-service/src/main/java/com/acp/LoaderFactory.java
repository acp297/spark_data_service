package com.acp;

import com.acp.azureSqlDataBase.SqlDbLoader;
import com.acp.azureSqlDataWarehouse.SqlDwLoader;
import com.acp.enums.SqlLoaderType;

public class LoaderFactory {

    public static Loader getLoader(SqlLoaderType sqlLoaderType){

        switch (sqlLoaderType){
            case AZURE_SQL_DW:
                return new SqlDwLoader();

            case AZURE_SQL_DB:
                return new SqlDbLoader();

            default:
                throw new IllegalArgumentException("Unknown loader type");
        }
    }
}
