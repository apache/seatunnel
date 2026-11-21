---
title: Anydoc 文档解析器 PoC
---

# Anydoc 文档解析器 PoC

本文是设计与兼容性 PoC，不会新增文档文件格式，也不会引入 anydoc 运行时依赖。

**状态：** 该开放方案由 GH-11801 跟踪。在运行时模型确定前，本文不会加入文档侧边栏发布。

## 当前边界

文件连接器目前有两条独立的文档处理路径：

- `MarkdownReadStrategy` 读取 Markdown 并输出文档元素行。
- `PdfReadStrategy` 使用 PDFBox 读取 PDF，并输出相近的行结构。

anydoc 后端应在现有 Markdown 行生成之前增加转换步骤：

```text
源文件字节
    -> 文档转 Markdown 后端
    -> MarkdownReadStrategy
    -> 文档元素行和可选的 RAG 元数据
```

本 PoC 为 `MarkdownReadStrategy` 增加包级可见的 Markdown 交接方法。交接方法显式接收原始源 URI 和原始源字节的 SHA-256，不能使用临时 Markdown 路径生成文档标识，也不能把转换后 Markdown 的哈希当作原始文档哈希。现有 Markdown 文件仍使用原有读取路径和结构。确定性单元测试使用 anydoc CLI 从已有 Excel 测试文件生成的 Markdown，验证现有元素和 RAG 元数据行为。

## PoC 命令

实验直接使用上游 CLI，不把它加入 SeaTunnel 依赖。由于 `npx -y` 会下载并执行指定软件包，只应在隔离的开发环境中运行：

```shell
npx -y @firecrawl/anydoc@0.1.9 \
  seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/excel/test_read_excel.xlsx \
  -o anydoc-output.md
```

该测试资源使用 `0.1.9` 版本生成，SHA-256 为 `e5fc44559f67b2970729b0fb4d4b2d71935790d3073e2abb9001d3b224826fa0`。输出包含 `Sheet1` 标题和 GitHub-Flavored Markdown 表格。测试资源保存该输出，使单元测试保持本地、确定且不依赖网络。

## 兼容性结论

- 现有 `markdown` 和 `pdf` 格式不需要改变。
- RAG 字段可以继续使用原始源 URI，因此文档和分块标识不会绑定到临时 Markdown 文件。
- 当前 Flexmark 解析器未启用表格扩展，因此 anydoc 生成的电子表格和 CSV 表格会退化为一个包含竖线标记的段落分块。要支持表格数据，必须兼容地启用 `TablesExtension`，或在转换层按表格结构分块，这不是可选优化。
- `MarkdownReadStrategy` 已包含 `TableBlock` 处理代码，但在禁用 `TablesExtension` 时无法到达。启用该代码前必须先确定兼容的行结构。
- 文本型文档可以转换；纯图片 PDF 仍需要 OCR，不能声明为本地 anydoc 后端支持的格式。

## 运行时选项

### 外部可执行文件

技术实现最小，但每个 worker 都必须安装固定版本的可执行文件。SeaTunnel 还需要定义命令发现、超时、进程终止、临时文件、输出限制、stderr、并发和后端缺失时的行为。

### WebAssembly

可以避免每个文档启动一次进程，但 SeaTunnel 当前没有 Java WebAssembly 运行时边界。引入前需要确定运行时、依赖与分发方式、内存和执行限制，并验证 Linux、macOS 和 Windows。

### Sidecar 服务

可以把原生运行时与 JVM 隔离，但会增加网络 API、部署、认证、重试和可用性约定，不等同于可选的本地解析器。

## 生产实现前需要确认的决策

在新增用户配置或 `DOCUMENT` 格式前，维护者需要确定支持的运行时与打包模型，包括：

1. 首个后端使用外部可执行文件、WebAssembly 还是 sidecar；
2. 谁负责在每个 worker 上安装并固定后端版本；
3. 支持的操作系统和处理器架构；
4. 超时、输出大小、资源、并发和清理限制；
5. 不支持、加密、损坏和纯图片文件的错误分类；
6. GFM 表格改为表格元素，还是在转换层进行分块，以及对现有 Markdown 输出的兼容性影响；
7. 许可证、NOTICE、二进制分发和漏洞更新责任。

完成上述决策后，生产实现可以增加文档解析器 SPI、一个可选 provider、声明式配置、英文和中文连接器文档及 E2E 测试，同时保持现有 PDF 和 Markdown 默认行为不变。
