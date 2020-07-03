package com.acp.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Configuration to establish connection with Azure Keyvault.
 *
 * @author Anand Prakash
 **/

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public final class KeyVaultConfig {
    private String secretScopeName;

}
