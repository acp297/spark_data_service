package com.acp.config;

import com.acp.constants.CommandLineParamConstants;
import com.acp.enums.Env;
import com.acp.helper.Util;
import com.acp.helper.YamlUtil;
import com.acp.models.Configuration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.acp.helper.Util.getCommandLine;

/**
 * Consists of all configurations required to be passed to run the program.
 *
 * @author Anand Prakash
 */
@Log4j
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JobConfig {
    private String env;
    private String jobType;
    private Map<String, String> jobConf;
    private String transformType;
    private String joinType;
    private Map<String, String> joinConditions;
    private String sourceFileType;
    private String[] sourcePaths;
    private String targetFileType;
    private String targetPath;
    private String schemaName;
    private String tableName;
    private Map<String, String> options;
    private String[] finalCols;
    private String[] primaryKeys;
    private SparkSession sparkSession;
    private Configuration configuration;
    private List<String> partitionColumns;
    private Map<String, String> sparkConf;
    private int repartitionValue;
    private List<String> sortColumns;

    /**
     * Loads the secrets from Azure Keyvault.
     * Reads the secrets from provided yaml file when it runs in local
     */
    public void loadSecrets(String path) throws IOException {
        Configuration config = YamlUtil.getConfigFromFile(path, Configuration.class);
        this.setConfiguration(config);
    }

    /**
     * Sets the job configuration and returns JobConfig object.
     *
     * @param args
     * @return
     * @throws ParseException
     */
    public static JobConfig getJobConfig(String[] args) throws ParseException, IOException {
        CommandLine cmd = getCommandLine(args);
        log.info("Initialising job config with arguments "+ StringUtils.join(args, ", "));
        JobConfig jobConfig = new JobConfig();

        String env = cmd.getOptionValue(CommandLineParamConstants.ENV);
        String sourcePath = cmd.getOptionValue(CommandLineParamConstants.SOURCE_PATH);
        String targetPath = cmd.getOptionValue(CommandLineParamConstants.TARGET_PATH);
        String primaryKeys = cmd.getOptionValue(CommandLineParamConstants.PRIMARY_KEYS);
        String finalColumns = cmd.getOptionValue(CommandLineParamConstants.FINAL_COLUMNS);
        String partitionColumns = cmd.getOptionValue(CommandLineParamConstants.PARTITION_COLUMNS);

        jobConfig.setEnv(env);
        jobConfig.setJobType(cmd.getOptionValue(CommandLineParamConstants.JOB_TYPE));
        jobConfig.setSourceFileType(cmd.getOptionValue(
                CommandLineParamConstants.SOURCE_FILE_TYPE));
        jobConfig.setTargetFileType(cmd.getOptionValue(
                CommandLineParamConstants.TARGET_FILE_TYPE));
        jobConfig.setJobConf(Util.extractKeyValue(cmd.getOptionValue(
                CommandLineParamConstants.JOB_CONF)));
        jobConfig.setPrimaryKeys(Util.parseStringToArray(primaryKeys));
        jobConfig.setSchemaName(cmd.getOptionValue(CommandLineParamConstants.SCHEMA_NAME));
        jobConfig.setTableName(cmd.getOptionValue(CommandLineParamConstants.TABLE_NAME));
        jobConfig.setFinalCols(Util.parseStringToArray(finalColumns));
        jobConfig.loadSecrets("src/main/resources/application.yaml");
        jobConfig.setPartitionColumns(Util.parseStringToList(partitionColumns));
        jobConfig.setSparkConf(Util.getMapFromString(cmd.getOptionValue(
                CommandLineParamConstants.SPARK_CONF)));

        String[] sourcePaths = Util.parseStringToArray(sourcePath);
        if (ArrayUtils.isEmpty(sourcePaths)){
            throw new IllegalArgumentException("Source path cannot be empty");
        }
        if (env.equalsIgnoreCase(Env.LOCAL.toString()) ||
                env.equalsIgnoreCase(Env.TEST.toString())) {
            jobConfig.setSourcePaths(sourcePaths);
            jobConfig.setTargetPath(targetPath);
        } else {
            String accountName = jobConfig.getConfiguration().getAdlsConfig().getAccountName();
            jobConfig.setSourcePaths(Util.generateAbfssPath(sourcePaths,
                    accountName));
            jobConfig.setTargetPath(Util.generateAbfssPath(targetPath, accountName));
        }
        String repartitionValue =
                cmd.getOptionValue(CommandLineParamConstants.REPARTITION_VALUE);
        jobConfig.setRepartitionValue(StringUtils.isEmpty(repartitionValue) ?
                0 : Integer.parseInt(repartitionValue));
        jobConfig.setSortColumns(Util.parseStringToList(
                cmd.getOptionValue(CommandLineParamConstants.SORT)));
        return jobConfig;
    }
}
