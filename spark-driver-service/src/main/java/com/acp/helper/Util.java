package com.acp.helper;

import com.acp.constants.AzureConstants;
import com.acp.constants.CommandLineParamConstants;
import com.acp.enums.Env;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;

import static com.acp.constants.CommandLineParamConstants.ENV;

/**
 * Util class to generate the path suitable for ADF pipeline.
 * It also gets command line arguments passed to run the program.
 * @author Anand Prakash
 **/

public class Util {

    /**
     * Generates path suitable to be added as parameter in ADF pipeline
     *
     * @param path
     * @return
     */
    public static String generateAbfssPath(String path, String accountName){
        if (StringUtils.isEmpty(path)){
            return null;
        }
        String[] pathArray= path.split("/",2);
        return AzureConstants.FILE_PROTOCOL + "://" + pathArray[0] +
                "@" + accountName + AzureConstants.AZURE_FILE_FQDN + "/" + pathArray[1];
    }

    public static String[] generateAbfssPath(String[] paths, String accountName){
        if (ArrayUtils.isEmpty(paths)){
            throw new IllegalArgumentException("Path cannot be empty or null");
        }
        return Arrays.stream(paths).map(path -> generateAbfssPath(path, accountName))
                .toArray(String[]::new);
    }

    public static Map<String, String> extractKeyValue(String expr) {
        return extractKeyValue(expr, "=");
    }

    public static Map<String, String> extractKeyValue(String expr, String splitRegex) {
        if (StringUtils.isEmpty(expr)){
            return null;
        }
        Map<String, String> map = new HashMap<>();
        String[] exprs = parseStringToArray(expr);
        for (String str : exprs) {
            String[] cols = str.split(splitRegex);
            map.put(cols[0], cols[1]);
        }
        return map;
    }

    public static String[] parseStringToArray(String str) {
        if (StringUtils.isEmpty(str)){
            return ArrayUtils.toArray();
        }
        return str.replaceAll("\\s", "").trim().split("\\s*,\\s*");
    }

    public static CommandLine getCommandLine(String[] args) throws ParseException {
        Options options = getOptions();
        CommandLineParser cmdParser = new GnuParser();
        CommandLine cmd = cmdParser.parse(options, args);
        commandLineOptionValidator(cmd);
        return cmd;
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("e", ENV, true,
                "Environment (valid options are: local, test, predev, dev, staging, prod");
        options.addOption("j", CommandLineParamConstants.JOB_TYPE, true, "Job type");
        options.addOption("jc", CommandLineParamConstants.JOB_CONF, true, "Job conf");
        options.addOption("sft",
                CommandLineParamConstants.SOURCE_FILE_TYPE, true, "source file type");
        options.addOption("tft",
                CommandLineParamConstants.TARGET_FILE_TYPE, true, "target file type");
        options.addOption("s", CommandLineParamConstants.SOURCE_PATH, true, "Source path");
        options.addOption("t", CommandLineParamConstants.TARGET_PATH, true, "target path");
        options.addOption("sn", CommandLineParamConstants.SCHEMA_NAME, true, "schema name");
        options.addOption("t", CommandLineParamConstants.TABLE_NAME, true, "table name");
        options.addOption("pk", CommandLineParamConstants.PRIMARY_KEYS, true, "primary key");
        options.addOption("fc", CommandLineParamConstants.FINAL_COLUMNS, true,
                "Final columns list");
        options.addOption("c", CommandLineParamConstants.SPARK_CONF, true, "Spark conf");
        options.addOption("rv", CommandLineParamConstants.REPARTITION_VALUE, true,
                "Number of partition in dataset repartition");
        options.addOption("pc", CommandLineParamConstants.PARTITION_COLUMNS, true,
                "partition columns");
        options.addOption("sc", CommandLineParamConstants.SORT, true,
                "sort columns");
        return options;
    }

    /**
     * Validates the command line options
     *
     * @param commandLine commandline variable
     */
    public static void commandLineOptionValidator(CommandLine commandLine) {

        if (!commandLine.hasOption(ENV)) {
            throw new RuntimeException("Environment value is not provided");
        }

        try {
            Env.valueOf(commandLine.getOptionValue(ENV).toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Environment value is not valid" +
                    e.getMessage());
        }

        if (!commandLine.hasOption(CommandLineParamConstants.JOB_TYPE)) {
            throw new RuntimeException("JobType value is not provided");
        }

        if (!commandLine.hasOption(CommandLineParamConstants.SOURCE_FILE_TYPE)) {
            throw new RuntimeException("fileType value is not provided");
        }

        if (!commandLine.hasOption(CommandLineParamConstants.SOURCE_PATH)) {
            throw new RuntimeException("sourcePath value is not provided");
        }
    }

    /**
     * Method to convert string to map.
     *
     * @param mapStr String with syntax as key1=value1,key2=value2
     * @return
     */
    public static Map<String, String> getMapFromString(String mapStr) {
        if (StringUtils.isEmpty(mapStr)){
            return null;
        }
        Map<String, String> joinCondition = new HashMap<>();
        String[] exprs = parseStringToArray(mapStr);
        for (String expr : exprs) {
            String[] cols = expr.split("\\s*=\\s*");
            joinCondition.put(cols[0], cols[1]);
        }
        return joinCondition;
    }

    /**
     * Method to create list form comma separated string
     * @param str
     * @return
     */
    public static List<String> parseStringToList(String str) {
        List<String> list = new LinkedList<>();
        if (StringUtils.isEmpty(str)){
            return list;
        }
        String[] tokens = str.trim().split("\\s*,\\s*");
        if(tokens != null && tokens.length > 0) {
            for (String column : tokens) {
                list.add(column);
            }
        }
        return list;
    }
}
