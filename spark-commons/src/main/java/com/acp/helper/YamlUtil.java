package com.acp.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * Loads configuration from yaml file.
 * @author Anand Prakash
 */

@Slf4j
@NoArgsConstructor
public class YamlUtil {

    /**
     * Loads configuration from file.
     */
    public static <T> T getConfigFromFile(String path, Class<T> clazz) throws IOException {
        T t;
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            t = mapper.readValue(new File(path), clazz);
        } catch (IOException e) {
            log.error("FilePath: " + path + "\nFolder path: "
                + System.getProperty("user.dir") + " " + e.getMessage());
            throw new IOException("Configuration loader failed" + " " + e.getMessage());
        }
        return t;
    }
} 
