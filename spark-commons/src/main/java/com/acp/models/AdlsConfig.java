package com.acp.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * ADLS Storage Account Configuration to establish connection.
 *
 * @author Anand Prakash
 **/

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public final class AdlsConfig {
    private String accountName;
    private String accountKey;
}
