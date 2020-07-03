package com.acp.config;

import com.acp.constants.SparkConfigConstants;
import com.acp.models.AdlsConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * Initializes singleton [[SparkSession]] and sets configuration
 *
 * This class creates a new instance of spark session.
 * It always assumes availability of existing spark session or creates new if it doesn't exists.
 * It also sets additional configuration to any given [[SparkSession]]
 * @author Anand Prakash
 */
@Slf4j
public enum SparkSessionConfig {

    /**
     * Singleton Instance spark session config.
     */
    INSTANCE;

    @Getter()
    private SparkSession sparkSession;

    /**
     * Set Additional spark configuration.
     *
     * @param sparkSession
     */
    public void setSparkConfig(SparkSession sparkSession, AdlsConfig adlsConfig) {
        String adlsConnectionKey = SparkConfigConstants.FS_AZURE +
                SparkConfigConstants.ACCOUNT_KEY +
                adlsConfig.getAccountName() + SparkConfigConstants.DFS_CORE_WINDOWS_NET;

        sparkSession.sparkContext().conf().set(adlsConnectionKey, adlsConfig.getAccountKey());
        sparkSession.sparkContext().hadoopConfiguration().set(adlsConnectionKey,
                adlsConfig.getAccountKey());
        sparkSession.sparkContext().hadoopConfiguration().set(SparkConfigConstants.FS_AZURE,
                SparkConfigConstants.HADOOP_FS_AZURE);

    }

    /**
     * Initialize [[SparkSession]] based on env.
     *
     * @param jobConfig
     * @return the spark session
     */
    public SparkSession initializeSparkSession(JobConfig jobConfig) {
        switch (jobConfig.getEnv()) {
            case "local":
                sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
                SparkSessionConfig.INSTANCE.setUserProvidedSparkConf(sparkSession,
                        jobConfig.getSparkConf());
                break;
            default:{
                sparkSession = SparkSession.builder().getOrCreate();
                SparkSessionConfig.INSTANCE.setSparkConfig(sparkSession,
                        jobConfig.getConfiguration().getAdlsConfig());
                SparkSessionConfig.INSTANCE.setUserProvidedSparkConf(sparkSession,
                        jobConfig.getSparkConf());
                break;
            }
        }
        return sparkSession;
    }

    /**
     * Method to set user provided spark conf
     * @param sparkSession
     * @param confs
     */
    public void setUserProvidedSparkConf(SparkSession sparkSession, Map<String, String> confs) {
        if(MapUtils.isEmpty(confs)) {
            return;
        }
        for(Map.Entry<String, String> conf : confs.entrySet()) {
            log.info("Setting user provided spark conf for property: "
                    +conf.getKey()+", value: "+ conf.getValue());
            sparkSession.conf().set(conf.getKey(), conf.getValue());
        }
    }
}

