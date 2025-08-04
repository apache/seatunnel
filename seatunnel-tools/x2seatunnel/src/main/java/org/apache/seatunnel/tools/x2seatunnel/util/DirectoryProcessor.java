package org.apache.seatunnel.tools.x2seatunnel.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** Batch processing directory scanning tool */
public class DirectoryProcessor {
    private final String inputDir;
    private final String outputDir;

    public DirectoryProcessor(String inputDir, String outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    /**
     * Get all files to be converted, filtered by extension (JSON/XML/TXT)
     *
     * @return list of file paths
     */
    public List<String> listSourceFiles() {
        List<String> result = new ArrayList<>();
        try {
            Files.walk(Paths.get(inputDir))
                    .filter(Files::isRegularFile)
                    .filter(
                            path -> {
                                String ext = FileUtils.getFileExtension(path.toString());
                                return "json".equals(ext) || "xml".equals(ext) || "txt".equals(ext);
                            })
                    .forEach(path -> result.add(path.toString()));
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan directory: " + inputDir, e);
        }
        return result;
    }

    /**
     * Generate the target file path based on the source file path
     *
     * @param sourceFile the path of the source file
     * @return the path of the target file
     */
    public String resolveTargetPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".conf").toString();
    }

    /**
     * Generate the report file path based on the source file path
     *
     * @param sourceFile the path of the source file
     * @return the path of the report file
     */
    public String resolveReportPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".md").toString();
    }
}
