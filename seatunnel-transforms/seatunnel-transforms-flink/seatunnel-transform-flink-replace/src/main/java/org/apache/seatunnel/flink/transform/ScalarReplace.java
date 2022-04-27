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
        String raw_text = str;
        if (isRegex) {
            if (replaceFirst) {
                raw_text = str.replaceFirst(pattern, replacement);
            } else {
                raw_text = str.replaceAll(pattern, replacement);
            }
        } else {
            raw_text = StringUtils.replace(str, pattern, replacement, replaceFirst ? 1 : -1);
        }
        return raw_text;
    }

}
