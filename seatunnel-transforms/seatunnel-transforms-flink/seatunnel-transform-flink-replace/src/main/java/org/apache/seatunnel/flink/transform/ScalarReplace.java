package org.apache.seatunnel.flink.transform;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class ScalarReplace extends ScalarFunction {

    private String pattern;
    private String replacement;
    private Boolean isRegex;
    private Boolean replaceFirst;

    public ScalarReplace(String pattern, String replacement, Boolean isRegex, Boolean replaceFirst) {
        this.pattern = pattern;
        this.replacement = replacement;
        this.isRegex = isRegex;
        this.replaceFirst = replaceFirst;
    }


    public String eval(String str) {
        String rawText = str;
        if (isRegex) {
            if (replaceFirst) {
                rawText = str.replaceFirst(pattern, replacement);
            } else {
                rawText = str.replaceAll(pattern, replacement);
            }
        } else {
            rawText = StringUtils.replace(str, pattern, replacement, replaceFirst ? 1 : -1);
        }
        return rawText;
    }

}
