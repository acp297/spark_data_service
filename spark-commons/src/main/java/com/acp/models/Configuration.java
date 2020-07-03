package com.acp.models;

import lombok.Getter;
import lombok.Setter;

/**
 * POJO class for Configuration.
 *
 * @author Anand Prakash
 */
@Getter
@Setter
public final class Configuration {
    private KeyVaultConfig keyVaultConfig;
    private AdlsConfig adlsConfig;
    private BaseConnectionConfig sqlConnectionConfig;
}
