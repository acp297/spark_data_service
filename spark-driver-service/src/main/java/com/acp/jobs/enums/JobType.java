package com.acp.jobs.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Type of job to run.
 */
@AllArgsConstructor
@Getter
public enum JobType {
    LOAD_TO_DELTALAKE("loadToDeltaLake"),
    LOAD_TO_MS_SQL_DW("loadToMsSqlDw"),
    LOAD_TO_MS_SQL_DB("loadToMsSqlDb");

    private final String name;
}
