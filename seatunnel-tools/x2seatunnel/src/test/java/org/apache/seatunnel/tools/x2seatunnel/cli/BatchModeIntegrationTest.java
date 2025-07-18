package org.apache.seatunnel.tools.x2seatunnel.cli;

import org.apache.seatunnel.tools.x2seatunnel.util.FileUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/** 集成测试：批量模式下转换多个示例文件 */
public class BatchModeIntegrationTest {

    @TempDir Path tempDir;

    @Test
    public void testBatchModeConversion() throws Exception {
        // 准备输入目录，将内置示例复制到临时目录
        Path inputDir = tempDir.resolve("input");
        Files.createDirectories(inputDir);
        Path examples = Paths.get("src", "main", "resources", "examples", "source");
        try (Stream<Path> paths = Files.list(examples)) {
            paths.filter(p -> p.toString().endsWith(".json"))
                    .forEach(
                            p -> {
                                try {
                                    Files.copy(p, inputDir.resolve(p.getFileName()));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
        }

        // 准备输出目录和报告路径
        Path outputDir = tempDir.resolve("output");
        String reportPath = outputDir.resolve("summary.md").toString();

        // 调用 CLI 批量模式
        String[] args =
                new String[] {
                    "-d", inputDir.toString(),
                    "-o", outputDir.toString(),
                    "-r", reportPath
                };
        X2SeaTunnelCli cli = new X2SeaTunnelCli();
        cli.run(args);

        // 验证所有输入文件对应的 .conf 文件已生成
        try (Stream<Path> paths = Files.list(inputDir)) {
            paths.filter(p -> p.toString().endsWith(".json"))
                    .forEach(
                            p -> {
                                String name =
                                        p.getFileName().toString().replaceAll("\\.json$", ".conf");
                                Path outFile = outputDir.resolve(name);
                                Assertions.assertTrue(Files.exists(outFile), "输出文件不存在: " + outFile);
                                // 检查 .conf 文件大小大于0
                                try {
                                    Assertions.assertTrue(
                                            Files.size(outFile) > 0, "输出文件为空: " + outFile);
                                } catch (IOException e) {
                                    Assertions.fail("无法获取输出文件大小: " + outFile);
                                }
                            });
        }

        // 验证汇总报告
        Assertions.assertTrue(Files.exists(Paths.get(reportPath)), "汇总报告不存在");
        String reportContent = FileUtils.readFile(reportPath);
        // 至少包含总数信息
        Assertions.assertTrue(reportContent.contains("## 成功转换"), "报告未包含成功转换部分");
        Assertions.assertTrue(reportContent.contains("## 转换失败"), "报告未包含失败转换部分");
    }
}
