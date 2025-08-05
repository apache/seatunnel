/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.tools.x2seatunnel.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Batch conversion report, records successful and failed entries and outputs a report file */
public class BatchConversionReport {

    private final List<ConversionRecord> successList = new ArrayList<>();
    private final Map<String, String> failureMap = new LinkedHashMap<>();

    private String sourceDirectory;
    private String outputDirectory;
    private String reportDirectory;
    private String filePattern;
    private String templatePath;
    private LocalDateTime startTime;
    private LocalDateTime endTime;

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

    public void recordSuccess(String sourceFile, String targetFile, String reportFile) {
        successList.add(new ConversionRecord(sourceFile, targetFile, reportFile));
    }

    public void recordSuccess(String source) {
        // For backward compatibility, generate default target and report file paths
        String targetFile = generateDefaultTargetPath(source);
        String reportFile = generateDefaultReportPath(source);
        recordSuccess(source, targetFile, reportFile);
    }

    public void recordFailure(String source, String reason) {
        failureMap.put(source, reason);
    }

    public void finish() {
        this.endTime = LocalDateTime.now();
    }

    private String generateDefaultTargetPath(String sourceFile) {
        if (outputDirectory != null) {
            String fileName = FileUtils.getFileNameWithoutExtension(sourceFile);
            return outputDirectory + "/" + fileName + ".conf";
        }
        return sourceFile.replace(".json", ".conf");
    }

    private String generateDefaultReportPath(String sourceFile) {
        if (reportDirectory != null) {
            String fileName = FileUtils.getFileNameWithoutExtension(sourceFile);
            return reportDirectory + "/" + fileName + ".md";
        }
        return sourceFile.replace(".json", ".md");
    }

    /**
     * Write report in Markdown format
     *
     * @param reportPath report file output path
     */
    public void writeReport(String reportPath) {
        if (endTime == null) {
            finish(); // If finish() was not called, complete automatically
        }

        StringBuilder sb = new StringBuilder();

        // Title and basic information
        sb.append("# Batch Conversion Report\n\n");
        sb.append("## 📋 Conversion Overview\n\n");
        sb.append("| Item | Value |\n");
        sb.append("|------|-------|\n");
        sb.append("| **Start Time** | ").append(formatDateTime(startTime)).append(" |\n");
        sb.append("| **End Time** | ").append(formatDateTime(endTime)).append(" |\n");
        sb.append("| **Duration** | ").append(calculateDuration()).append(" |\n");
        sb.append("| **Source Directory** | `")
                .append(sourceDirectory != null ? sourceDirectory : "Not specified")
                .append("` |\n");
        sb.append("| **Output Directory** | `")
                .append(outputDirectory != null ? outputDirectory : "Not specified")
                .append("` |\n");
        sb.append("| **Report Directory** | `")
                .append(reportDirectory != null ? reportDirectory : "Not specified")
                .append("` |\n");
        sb.append("| **File Pattern** | `")
                .append(filePattern != null ? filePattern : "*.json")
                .append("` |\n");
        sb.append("| **Custom Template** | `")
                .append(templatePath != null ? templatePath : "Default template")
                .append("` |\n");
        sb.append("| **Successful Conversions** | ")
                .append(successList.size())
                .append(" files |\n");
        sb.append("| **Failed Conversions** | ").append(failureMap.size()).append(" files |\n");
        sb.append("| **Total** | ")
                .append(successList.size() + failureMap.size())
                .append(" files |\n");
        sb.append("| **Success Rate** | ").append(calculateSuccessRate()).append(" |\n\n");

        // Successful conversion details
        sb.append("## ✅ Successful Conversions (").append(successList.size()).append(")\n\n");
        if (successList.isEmpty()) {
            sb.append("*No successfully converted files*\n\n");
        } else {
            sb.append("| # | Source File | Target File | Report File |\n");
            sb.append("|---|-------------|-------------|-------------|\n");
            for (int i = 0; i < successList.size(); i++) {
                ConversionRecord record = successList.get(i);
                sb.append("| ").append(i + 1).append(" | ");
                sb.append("`").append(record.getSourceFile()).append("` | ");
                sb.append("`").append(record.getTargetFile()).append("` | ");
                sb.append("`").append(record.getReportFile()).append("` |\n");
            }
            sb.append("\n");
        }

        // Failed conversion details
        sb.append("## ❌ Failed Conversions (").append(failureMap.size()).append(")\n\n");
        if (failureMap.isEmpty()) {
            sb.append("*No failed conversion files*\n\n");
        } else {
            sb.append("| # | Source File | Failure Reason |\n");
            sb.append("|---|-------------|----------------|\n");
            int index = 1;
            for (Map.Entry<String, String> entry : failureMap.entrySet()) {
                sb.append("| ").append(index++).append(" | ");
                sb.append("`").append(entry.getKey()).append("` | ");
                sb.append(entry.getValue()).append(" |\n");
            }
            sb.append("\n");
        }

        // Add simple footer information
        sb.append("---\n");
        sb.append("*Report generated at: ")
                .append(formatDateTime(LocalDateTime.now()))
                .append("*\n");
        sb.append("*Tool version: X2SeaTunnel v0.1*\n");

        // Write to file
        FileUtils.writeFile(reportPath, sb.toString());
    }

    /** Format date time */
    private String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null) {
            return "Unknown";
        }
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /** Calculate conversion duration */
    private String calculateDuration() {
        if (startTime == null || endTime == null) {
            return "Unknown";
        }

        long seconds = java.time.Duration.between(startTime, endTime).getSeconds();
        if (seconds < 60) {
            return seconds + " seconds";
        } else if (seconds < 3600) {
            return (seconds / 60) + " minutes " + (seconds % 60) + " seconds";
        } else {
            long hours = seconds / 3600;
            long minutes = (seconds % 3600) / 60;
            long remainingSeconds = seconds % 60;
            return hours + " hours " + minutes + " minutes " + remainingSeconds + " seconds";
        }
    }

    /** Calculate success rate */
    private String calculateSuccessRate() {
        int total = successList.size() + failureMap.size();
        if (total == 0) {
            return "0%";
        }
        double rate = (double) successList.size() / total * 100;
        return String.format("%.1f%%", rate);
    }
}
