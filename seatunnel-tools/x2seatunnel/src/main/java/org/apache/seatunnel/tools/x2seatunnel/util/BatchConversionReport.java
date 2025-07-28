package org.apache.seatunnel.tools.x2seatunnel.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 批量转换报告，记录成功和失败条目并输出报告文件 */
public class BatchConversionReport {

    // 成功转换的记录
    private final List<ConversionRecord> successList = new ArrayList<>();
    // 失败转换的记录
    private final Map<String, String> failureMap = new LinkedHashMap<>();

    // 批量转换的配置信息
    private String sourceDirectory;
    private String outputDirectory;
    private String reportDirectory;
    private String filePattern;
    private String templatePath;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    /** 转换记录 */
    public static class ConversionRecord {
        private final String sourceFile;
        private final String targetFile;
        private final String reportFile;
        private final LocalDateTime convertTime;

        public ConversionRecord(String sourceFile, String targetFile, String reportFile) {
            this.sourceFile = sourceFile;
            this.targetFile = targetFile;
            this.reportFile = reportFile;
            this.convertTime = LocalDateTime.now();
        }

        public String getSourceFile() {
            return sourceFile;
        }

        public String getTargetFile() {
            return targetFile;
        }

        public String getReportFile() {
            return reportFile;
        }

        public LocalDateTime getConvertTime() {
            return convertTime;
        }
    }

    /** 设置批量转换配置信息 */
    public void setConversionConfig(
            String sourceDirectory,
            String outputDirectory,
            String reportDirectory,
            String filePattern,
            String templatePath) {
        this.sourceDirectory = sourceDirectory;
        this.outputDirectory = outputDirectory;
        this.reportDirectory = reportDirectory;
        this.filePattern = filePattern;
        this.templatePath = templatePath;
        this.startTime = LocalDateTime.now();
    }

    /** 记录成功的转换 */
    public void recordSuccess(String sourceFile, String targetFile, String reportFile) {
        successList.add(new ConversionRecord(sourceFile, targetFile, reportFile));
    }

    /** 记录成功的转换（向后兼容） */
    public void recordSuccess(String source) {
        // 为了向后兼容，生成默认的目标和报告文件路径
        String targetFile = generateDefaultTargetPath(source);
        String reportFile = generateDefaultReportPath(source);
        recordSuccess(source, targetFile, reportFile);
    }

    /** 记录失败的源文件路径和原因 */
    public void recordFailure(String source, String reason) {
        failureMap.put(source, reason);
    }

    /** 完成批量转换 */
    public void finish() {
        this.endTime = LocalDateTime.now();
    }

    /** 生成默认的目标文件路径 */
    private String generateDefaultTargetPath(String sourceFile) {
        if (outputDirectory != null) {
            String fileName = FileUtils.getFileNameWithoutExtension(sourceFile);
            return outputDirectory + "/" + fileName + ".conf";
        }
        return sourceFile.replace(".json", ".conf");
    }

    /** 生成默认的报告文件路径 */
    private String generateDefaultReportPath(String sourceFile) {
        if (reportDirectory != null) {
            String fileName = FileUtils.getFileNameWithoutExtension(sourceFile);
            return reportDirectory + "/" + fileName + ".md";
        }
        return sourceFile.replace(".json", ".md");
    }

    /**
     * 将报告写为 Markdown 格式
     *
     * @param reportPath 报告文件输出路径
     */
    public void writeReport(String reportPath) {
        if (endTime == null) {
            finish(); // 如果没有调用 finish()，自动完成
        }

        StringBuilder sb = new StringBuilder();

        // 标题和基本信息
        sb.append("# 批量转换报告\n\n");
        sb.append("## 📋 转换概览\n\n");
        sb.append("| 项目 | 值 |\n");
        sb.append("|------|----|\n");
        sb.append("| **开始时间** | ").append(formatDateTime(startTime)).append(" |\n");
        sb.append("| **结束时间** | ").append(formatDateTime(endTime)).append(" |\n");
        sb.append("| **耗时** | ").append(calculateDuration()).append(" |\n");
        sb.append("| **源目录** | `")
                .append(sourceDirectory != null ? sourceDirectory : "未指定")
                .append("` |\n");
        sb.append("| **输出目录** | `")
                .append(outputDirectory != null ? outputDirectory : "未指定")
                .append("` |\n");
        sb.append("| **报告目录** | `")
                .append(reportDirectory != null ? reportDirectory : "未指定")
                .append("` |\n");
        sb.append("| **文件模式** | `")
                .append(filePattern != null ? filePattern : "*.json")
                .append("` |\n");
        sb.append("| **自定义模板** | `")
                .append(templatePath != null ? templatePath : "默认模板")
                .append("` |\n");
        sb.append("| **成功转换** | ").append(successList.size()).append(" 个文件 |\n");
        sb.append("| **转换失败** | ").append(failureMap.size()).append(" 个文件 |\n");
        sb.append("| **总计** | ").append(successList.size() + failureMap.size()).append(" 个文件 |\n");
        sb.append("| **成功率** | ").append(calculateSuccessRate()).append(" |\n\n");

        // 成功转换详情
        sb.append("## ✅ 成功转换 (").append(successList.size()).append(")\n\n");
        if (successList.isEmpty()) {
            sb.append("*无成功转换的文件*\n\n");
        } else {
            sb.append("| # | 源文件 | 目标文件 | 报告文件 |\n");
            sb.append("|---|--------|----------|----------|\n");
            for (int i = 0; i < successList.size(); i++) {
                ConversionRecord record = successList.get(i);
                sb.append("| ").append(i + 1).append(" | ");
                sb.append("`").append(record.getSourceFile()).append("` | ");
                sb.append("`").append(record.getTargetFile()).append("` | ");
                sb.append("`").append(record.getReportFile()).append("` |\n");
            }
            sb.append("\n");
        }

        // 失败转换详情
        sb.append("## ❌ 转换失败 (").append(failureMap.size()).append(")\n\n");
        if (failureMap.isEmpty()) {
            sb.append("*无转换失败的文件*\n\n");
        } else {
            sb.append("| # | 源文件 | 失败原因 |\n");
            sb.append("|---|--------|----------|\n");
            int index = 1;
            for (Map.Entry<String, String> entry : failureMap.entrySet()) {
                sb.append("| ").append(index++).append(" | ");
                sb.append("`").append(entry.getKey()).append("` | ");
                sb.append(entry.getValue()).append(" |\n");
            }
            sb.append("\n");
        }

        // 添加简单的结尾信息
        sb.append("---\n");
        sb.append("*报告生成时间: ").append(formatDateTime(LocalDateTime.now())).append("*\n");
        sb.append("*工具版本: X2SeaTunnel v0.1*\n");

        // 写入文件
        FileUtils.writeFile(reportPath, sb.toString());
    }

    /** 格式化日期时间 */
    private String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null) {
            return "未知";
        }
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /** 计算转换耗时 */
    private String calculateDuration() {
        if (startTime == null || endTime == null) {
            return "未知";
        }

        long seconds = java.time.Duration.between(startTime, endTime).getSeconds();
        if (seconds < 60) {
            return seconds + " 秒";
        } else if (seconds < 3600) {
            return (seconds / 60) + " 分 " + (seconds % 60) + " 秒";
        } else {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            long remainingSeconds = seconds % 60;
            return hours + " 时 " + minutes + " 分 " + remainingSeconds + " 秒";
        }
    }

    /** 计算成功率 */
    private String calculateSuccessRate() {
        int total = successList.size() + failureMap.size();
        if (total == 0) {
            return "0%";
        }
        double rate = (double) successList.size() / total * 100;
        return String.format("%.1f%%", rate);
    }
}
