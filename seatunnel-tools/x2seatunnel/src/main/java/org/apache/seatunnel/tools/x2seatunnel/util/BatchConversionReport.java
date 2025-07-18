package org.apache.seatunnel.tools.x2seatunnel.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 批量转换报告，记录成功和失败条目并输出报告文件 */
public class BatchConversionReport {
    private final List<String> successList = new ArrayList<>();
    private final Map<String, String> failureMap = new LinkedHashMap<>();

    /** 记录成功的源文件路径 */
    public void recordSuccess(String source) {
        successList.add(source);
    }

    /** 记录失败的源文件路径和原因 */
    public void recordFailure(String source, String reason) {
        failureMap.put(source, reason);
    }

    /**
     * 将报告写为 Markdown 格式
     *
     * @param reportPath 报告文件输出路径
     */
    public void writeReport(String reportPath) {
        StringBuilder sb = new StringBuilder();
        sb.append("# 批量转换报告\n\n");
        sb.append("## 成功转换 (" + successList.size() + ")\n");
        for (String s : successList) {
            sb.append("- ✅ ").append(s).append("\n");
        }
        sb.append("\n");
        sb.append("## 转换失败 (" + failureMap.size() + ")\n");
        for (Map.Entry<String, String> entry : failureMap.entrySet()) {
            sb.append("- ❌ ")
                    .append(entry.getKey())
                    .append(" -> ")
                    .append(entry.getValue())
                    .append("\n");
        }
        // 写入文件
        FileUtils.writeFile(reportPath, sb.toString());
    }
}
