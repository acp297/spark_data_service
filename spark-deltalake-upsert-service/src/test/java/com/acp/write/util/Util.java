package com.acp.write.util;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Util {

    public static Dataset<Row> generateSampleDataset(SparkSession sparkSession){
        StructField[] structFields = new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty())
        };

        StructType structType = new StructType(structFields);

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "name1"));
        rows.add(RowFactory.create(2, "name2"));

        return sparkSession.createDataFrame(rows, structType);
    }

    public static SparkSession getSparkSession(){
        return SparkSession.builder().master("local").getOrCreate();
    }

    public static Dataset<Row> loadCsv(String path, Map<String, String> options) {
        options = MapUtils.emptyIfNull(options);
        return getSparkSession().read().options(options).format("csv").load(path);
    }

    /**
     * Util method to delete test files
     * @param path
     * @throws IOException
     */
    public static void deleteFile(String path) throws IOException {
        File delta = new File(path);
        if (delta.exists()) {
            FileUtils.deleteDirectory(delta);
        }
    }
}
