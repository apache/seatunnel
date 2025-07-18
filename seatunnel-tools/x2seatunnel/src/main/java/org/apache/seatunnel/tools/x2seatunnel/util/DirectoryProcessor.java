package org.apache.seatunnel.tools.x2seatunnel.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/** 批量处理目录扫描工具 */
public class DirectoryProcessor {
    private final String inputDir;
    private final String outputDir;

    public DirectoryProcessor(String inputDir, String outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    /**
     * 获取所有待转换文件列表，按扩展名过滤 (JSON/XML/TXT)
     *
     * @return 文件路径列表
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
            throw new RuntimeException("扫描目录失败: " + inputDir, e);
        }
        return result;
    }

    /**
     * 根据源文件路径生成目标文件路径
     *
     * @param sourceFile 源文件路径
     * @return 目标文件路径
     */
    public String resolveTargetPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".conf").toString();
    }

    /**
     * 根据源文件路径生成报告文件路径
     *
     * @param sourceFile 源文件路径
     * @return 报告文件路径
     */
    public String resolveReportPath(String sourceFile) {
        String name = FileUtils.getFileNameWithoutExtension(sourceFile);
        return Paths.get(outputDir, name + ".md").toString();
    }
}
