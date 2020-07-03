package com.acp;

import com.acp.config.JobConfig;
import com.acp.config.SparkSessionConfig;
import com.acp.jobs.JobFactory;
import com.acp.jobs.enums.JobType;
import com.acp.jobs.job.Job;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Driver class for Spark-Data-Services
 *
 * @author Anand Prakash
 */
@Slf4j
public class App {
    public static void main(String[] args) throws ParseException, SQLException, IOException {
        new App().run(args);
    }

    /**
     * Runs main functionality of data orchestration.
     * @param args the args
     * @throws ParseException the parse exception
     */
    public void run(String[] args) throws ParseException, SQLException, IOException {
        JobConfig jobConfig = JobConfig.getJobConfig(args);

        SparkSession sparkSession = SparkSessionConfig.INSTANCE.initializeSparkSession(jobConfig);
        jobConfig.setSparkSession(sparkSession);

        Job job = JobFactory.getJobInstance(JobType.valueOf(jobConfig.getJobType().toUpperCase()));
        job.init(jobConfig);
        job.run();
    }
}
