package com.acp.models;


import lombok.Builder;
import lombok.Getter;


/**
 * Connection configuration required for sql insertion.
 * @author Anand Prakash
 */
@Getter
public class ConnectionConfig extends BaseConnectionConfig {
    private String jdbcUrl;
    private String format;
    private String encrypt;
    private String trustServerCertificate;
    private String hostNameInCertificate;
    private String loginTimeout;

    @Builder
    public ConnectionConfig(String url, String database, String username,
                            String password, String jdbcUrl, String format,
                            String encrypt, String trustServerCertificate,
                            String hostNameInCertificate, String loginTimeout) {
        super(url, database, username, password);
        this.jdbcUrl = jdbcUrl;
        this.format = format;
        this.encrypt = encrypt;
        this.trustServerCertificate = trustServerCertificate;
        this.hostNameInCertificate = hostNameInCertificate;
        this.loginTimeout = loginTimeout;
    }
}
