package org.apache.seatunnel.tools.x2seatunnel.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FilePattern {

    /**
     * Filters the file list according to the wildcard patterns separated by commas.
     *
     * @param files The list of all file paths.
     * @param patterns The wildcard patterns, such as "*.json,*.xml".
     * @return The list of files that match the patterns.
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
