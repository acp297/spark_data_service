package com.acp.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The File type enum for different input / output / transformation
 */
@AllArgsConstructor
@Getter
public enum FileTypeEnum {

    AVRO(0, "avro"),
    PARQUET(1, "parquet"),
    JSON(2, "json"),
    CSV(3, "csv"),
    DELTA(4, "delta"),
    SQLDW(4, "sqldw"),
    UNKNOWN(5, "unknown");


    private Integer id;
    private String name;

}
