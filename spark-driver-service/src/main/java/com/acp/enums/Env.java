package com.acp.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Env {
    LOCAL,
    TEST,
    PREDEV,
    DEV,
    STAGING,
    PROD;
}
