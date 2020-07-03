package com.acp.models;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * Base configuration for sql connection.
 * @author Anand Prakash
 */

@Getter
@AllArgsConstructor
public class BaseConnectionConfig {
    private String url;
    private String database;
    private String username;
    private String password;
}
