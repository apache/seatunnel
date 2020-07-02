package io.github.interestinglab.waterdrop.common.utils;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StringTemplate {

    /**
     * @param str raw string
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * @return replaced string
     * */
    public static String substitute(String str, String timeFormat) {

        final SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        final String formattedDate = sdf.format(new Date());

        final Map valuesMap = new HashMap(5);
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(str);
    }
}

