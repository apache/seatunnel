package io.github.interestinglab.waterdrop.common.config;


import lombok.Data;

/**
 * @author mr_xiong
 * @date 2019-05-28 16:07
 * @description
 */
@Data
public class CheckResult {

    private boolean success;

    private String msg;

    public CheckResult(boolean success, String msg) {
        this.success = success;
        this.msg = msg;
    }
}
