package io.github.interestinglab.waterdrop.common.config;


import lombok.Data;

@Data
public class CheckResult {

    private boolean success;

    private String msg;

    public CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }
}
