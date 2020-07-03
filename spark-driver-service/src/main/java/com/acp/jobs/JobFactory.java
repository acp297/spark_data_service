package com.acp.jobs;

import com.acp.jobs.enums.JobType;
import com.acp.jobs.job.Job;
import com.acp.jobs.job.LoadToDeltaLake;
import com.acp.jobs.job.LoadToSql;

/**
 * Factory class to fetch the instance of Job impl class
 * @author Anand Prakash
 */

public class JobFactory {

    public static Job getJobInstance(JobType jobType) {
        switch (jobType) {
            case LOAD_TO_DELTALAKE:
                return new LoadToDeltaLake();

            case LOAD_TO_MS_SQL_DW:
            case LOAD_TO_MS_SQL_DB:
                return new LoadToSql();

            default:
                throw new IllegalArgumentException("jobType is invalid : " + jobType);
        }
    }
}
