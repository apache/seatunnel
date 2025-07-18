package org.apache.seatunnel.tools.x2seatunnel.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** 文件通配符匹配工具 */
public class FilePattern {

    /**
     * 根据逗号分隔的通配符模式过滤文件列表
     *
     * @param files 全部文件路径列表
     * @param patterns 通配符模式，如 "*.json,*.xml"
     * @return 匹配后的文件列表
     */
    public static List<String> filter(List<String> files, String patterns) {
        if (patterns == null || patterns.trim().isEmpty()) {
            return files;
        }
        String[] pats = patterns.split(",");
        List<Pattern> regexList = new ArrayList<>();
        for (String p : pats) {
            String pat = p.trim().replace(".", "\\.").replace("*", ".*");
            regexList.add(Pattern.compile(pat));
        }
        return files.stream()
                .filter(f -> regexList.stream().anyMatch(r -> r.matcher(f).matches()))
                .collect(Collectors.toList());
    }
}
