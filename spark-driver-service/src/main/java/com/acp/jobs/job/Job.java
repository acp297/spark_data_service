package com.acp.jobs.job;


import com.acp.config.JobConfig;

import java.sql.SQLException;

public interface Job {
    void run() throws SQLException;
    void init(JobConfig jobConfig);
}
