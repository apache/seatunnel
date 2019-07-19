package io.github.interestinglab.waterdrop.plugin;


import lombok.Data;

/**
 * @author mr_xiong
 * @date 2019-05-28 16:07
 * @description
 */
@Data
public class CheckResult {

    private boolean result;

    private String msg;

    public CheckResult(boolean result, String msg) {
        this.result = result;
        this.msg = msg;
    }
}
