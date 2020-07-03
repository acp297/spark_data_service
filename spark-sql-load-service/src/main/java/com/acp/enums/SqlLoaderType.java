package com.acp.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SqlLoaderType {
    AZURE_SQL_DB("azureSqlDb"),
    AZURE_SQL_DW("azureSqlDw");

    private final String name;
}
