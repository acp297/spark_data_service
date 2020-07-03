package com.acp.write;

import com.acp.write.util.Util;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.acp.constants.SparkOptions.MERGE_SCHEMA;
import static junit.framework.TestCase.assertEquals;
import static org.apache.spark.sql.functions.col;

/**
 * Test class for UpsertTransformer
 */
public class DeltaLakeUpsertWriterTest {

    private SparkSession sparkSession;
    private DeltaLakeUpsertWriter deltaLakeUpsertWriter;
    private HashMap<String, String> columnsMapping;
    private Dataset<Row> ds1;
    private Dataset<Row> ds2;
    private Dataset<Row> ds3;
    private Dataset<Row> ds4;
    private Dataset<Row> ds5;
    private Dataset<Row> ds6;
    private Dataset<Row> ds7;
    private Dataset<Row> ds8;

    @Before
    public void setUp() {
        sparkSession = Util.getSparkSession();
        ds1 = Util.loadCsv("src/test/resources/source1.csv", null)
                .toDF("col1", "col2", "col3");
        ds2 = Util.loadCsv  ("src/test/resources/source2.csv", null)
                .toDF("col1", "col2", "col3");

        ds3 = Util.loadCsv("src/test/resources/source3.csv", null)
                .toDF("col1", "col2", "col3");

        ds4 =  Util.loadCsv  ("src/test/resources/source4.csv", null)
                .toDF("col1", "col2", "col3");

        ds5 = Util.loadCsv("src/test/resources/source5.csv", null)
                .toDF("col1", "col2", "col3", "col4", "col5");

        ds6 = Util.loadCsv("src/test/resources/source6.csv", null)
                .toDF("col1", "col2", "col3");
        Map<String, String> options = new HashMap<>();
        options.put("header", "true");
        ds7 = Util.loadCsv("src/test/resources/source7.csv", options);
        ds8 = Util.loadCsv("src/test/resources/source8.csv", options);
    }

    @After
    public void tearDown() throws IOException {
        Util.deleteFile("src/test/resources/test");
        Util.deleteFile("src/test/resources/out");
    }

    @Test
    public void testUpsertWithSameSchema(){
        columnsMapping = new HashMap<>();
        columnsMapping.put("col1", "col1");

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/delta/")
                .setDs(ds1)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/delta/")
                .setDs(ds2)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> expectedDs = sparkSession.read().format("delta").load("src/test/resources/test/delta/");
        expectedDs.persist();
        assertEquals(5, expectedDs.count());
        assertEquals(1, expectedDs.filter("col1='01' AND col2='updated'").count());
    }

    @Test
    public void testUpsertWhenSourceHasAdditionalColumns(){


        ds5 = ds5.withColumn("col5", col("col5").cast(DataTypes.LongType));

        columnsMapping = new HashMap<>();
        columnsMapping.put("col1", "col1");


        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/deltaTest1/")
                .setDs(ds1)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/deltaTest1/")
                .setDs(ds5)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> expectedDs = sparkSession.read().format("delta").load("src/test/resources/test/deltaTest1/");
        expectedDs.persist();
        assertEquals(5, expectedDs.count());
        assertEquals(1, expectedDs.filter("col1='01' AND col2='abcUpdated' AND col3='cdeUpdated' AND col4='yyy' AND col5=1").count());
        assertEquals(1, expectedDs.filter("col1='04' AND col2='kkk' AND col3='mmm' AND col4='ppp' AND col5=4").count());
        assertEquals(1, expectedDs.filter("col1='02' AND col2='aaa' AND col3='bbb' AND col4 is null AND col5 is null").count());
        assertEquals(1, expectedDs.filter(col("_created_at").notEqual(col("_updated_at"))).count());
        assertEquals(4, expectedDs.filter(col("_created_at").equalTo(col("_updated_at"))).count());
    }

    @Test
    public void testUpsertWhenTargetHasAdditionalColumns(){

        ds5 = ds5.withColumn("col5", col("col5").cast(DataTypes.LongType));

        columnsMapping = new HashMap<>();
        columnsMapping.put("col1", "col1");


        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/deltaTest2/")
                .setDs(ds5)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/deltaTest2/")
                .setDs(ds6)
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> expectedDs = sparkSession.read().format("delta").load("src/test/resources/test/deltaTest2/");
        expectedDs.persist();
        assertEquals(4, expectedDs.count());
        assertEquals(1, expectedDs.filter("col1='01' AND col2='abcUP' AND col3='cdeUP' AND col4 is null AND col5 is null").count());
        assertEquals(1, expectedDs.filter("col1='04' AND col2='kkk' AND col3='mmm' AND col4='ppp' AND col5=4").count());
        assertEquals(1, expectedDs.filter("col1='05' AND col2='zzzUP' AND col3='dddUP' AND col4 is null AND col5 is null").count());
        assertEquals(2, expectedDs.filter(col("_created_at").notEqual(col("_updated_at"))).count());
        assertEquals(2, expectedDs.filter(col("_created_at").equalTo(col("_updated_at"))).count());
    }

    @Test
    public void testUpsertWithMultiplePrimaryKeys(){

        columnsMapping = new HashMap<>();
        columnsMapping.put("col1", "col1");
        columnsMapping.put("col2", "col2");

        Map<String, String> options = new HashMap<>();
        options.put(MERGE_SCHEMA, "true");
        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/delta1/")
                .setDs(ds3)
                .joinOnColumns(columnsMapping)
                .options(options)
                .build();
        deltaLakeUpsertWriter.upsert();

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/test/delta1/")
                .setDs(ds4)
                .joinOnColumns(columnsMapping)
                .columnsToInsert(null)
                .columnsToUpsert(null)
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> expectedDs = sparkSession.read().format("delta").load("src/test/resources/test/delta1/");
        expectedDs.persist();
        assertEquals(6, expectedDs.count());
        assertEquals(1, expectedDs.filter("col1='01' AND col2='abc' AND col3='updated'").count());
    }


    @Test
    public void testBuildMergeCondition(){
        TreeMap<String, String> cols = new TreeMap<>();
        cols.put("col1", "col1");
        cols.put("col2", "col2");
        cols.put("col3", "col3");

        String ExpectedCondition = new DeltaLakeUpsertWriter().buildMergeCondition(cols);
        assertEquals(ExpectedCondition,
                "source.col1=target.col1 and source.col2=target.col2 and source.col3=target.col3");
    }


    /**
     * Method to test number of records written with partition columns in save dataset
     */
    @Test
    public void testPartition(){
        columnsMapping = new HashMap<>();
        columnsMapping.put("id", "id");

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/out/delta/")
                .setDs(ds7)
                .partitionColumns(Lists.newArrayList("gender"))
                .joinOnColumns(columnsMapping)
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> totalDS = sparkSession.read().format("delta").load("src/test/resources/out/delta/");
        Dataset<Row> maleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta/gender=Male");
        Dataset<Row> femaleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta/gender=Female");
        assertEquals(5, totalDS.count());
        assertEquals(0, totalDS.filter("id='5' AND first_name='Terry'").count());
        assertEquals(3, maleDS.count());
        assertEquals(2, femaleDS.count());
    }

    /**
     * Method to test number of records written with partition columns in upsert service
     */
    @Test
    public void testUpsertWithPartition(){
        columnsMapping = new HashMap<>();
        columnsMapping.put("id", "id");

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/out/delta_1/")
                .setDs(ds7)
                .partitionColumns(Lists.newArrayList("gender"))
                .joinOnColumns(columnsMapping)
                .sortColumns(Lists.newArrayList("department"))
                .build();
        deltaLakeUpsertWriter.upsert();

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/out/delta_1/")
                .setDs(ds8)
                .partitionColumns(Lists.newArrayList("gender"))
                .joinOnColumns(columnsMapping)
                .sortColumns(Lists.newArrayList("department"))
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> totalDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_1/");
        Dataset<Row> maleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_1/gender=Male");
        Dataset<Row> femaleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_1/gender=Female");
        Dataset<Row> transDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_1/gender=Trans");
        assertEquals(10, totalDS.count());
        assertEquals(1, totalDS.filter("id='5' AND first_name='Terry'").count());
        assertEquals(6, maleDS.count());
        assertEquals(3, femaleDS.count());
        assertEquals(1, transDS.count());
    }

    /**
     * Method to test number of records written with multiple partitions columns in save dataset
     */
    @Test
    public void testMultiplePartition(){
        columnsMapping = new HashMap<>();
        columnsMapping.put("id", "id");

        deltaLakeUpsertWriter = UpsertBuilder.builder()
                .sparkSession(sparkSession)
                .deltaLocation("src/test/resources/out/delta_2/")
                .setDs(ds7)
                .partitionColumns(Lists.newArrayList("gender", "department"))
                .joinOnColumns(columnsMapping)
                .sortColumns(Lists.newArrayList("department"))
                .build();
        deltaLakeUpsertWriter.upsert();

        Dataset<Row> totalDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_2/");
        Dataset<Row> maleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_2/gender=Male/department=Tech");
        Dataset<Row> femaleDS = sparkSession.read().format("delta").load("src/test/resources/out/delta_2/gender=Female/department=Marketing");
        assertEquals(5, totalDS.count());
        assertEquals(1, totalDS.filter("id='5' AND first_name='Rockey'").count());
        assertEquals(2, maleDS.count());
        assertEquals(1, femaleDS.count());
    }

}


