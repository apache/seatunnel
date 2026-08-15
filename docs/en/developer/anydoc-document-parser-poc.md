---
title: Anydoc document parser PoC
---

# Anydoc document parser PoC

This is a design and compatibility PoC. It does not add a document file format or an anydoc runtime dependency.

## Current boundary

The file connector currently owns two separate document paths:

- `MarkdownReadStrategy` reads Markdown and emits document-element rows.
- `PdfReadStrategy` reads PDF files through PDFBox and emits a similar row schema.

An anydoc backend would add a conversion step before the existing Markdown row generation:

```text
source file bytes
    -> document-to-Markdown backend
    -> MarkdownReadStrategy
    -> document-element rows and optional RAG metadata
```

The PoC adds a package-private Markdown handoff method to `MarkdownReadStrategy`. Existing Markdown files still use the same read path and schema. A deterministic test feeds Markdown produced by the anydoc CLI from the existing Excel fixture into that method and verifies the existing element and RAG metadata behavior.

## PoC command

The experiment used the upstream CLI without adding it to SeaTunnel's dependencies:

```shell
npx -y @firecrawl/anydoc \
  seatunnel-connectors-v2/connector-file/connector-file-base/src/test/resources/excel/test_read_excel.xlsx \
  -o anydoc-output.md
```

The result contains a `Sheet1` heading and a GitHub-Flavored Markdown table. The checked-in test fixture preserves that output so unit tests remain local and deterministic.

## Compatibility findings

- Existing `markdown` and `pdf` formats do not need to change.
- The existing RAG fields can use the original source URI, so document and chunk identifiers remain tied to the source document instead of a temporary Markdown file.
- The current Flexmark parser does not enable the table extension. Anydoc tables therefore remain paragraph text. Enabling the extension would change existing Markdown row output and should be a separate compatibility decision.
- Text-based documents can be converted. Image-only PDFs still require OCR and must not be presented as supported by a local anydoc backend.

## Runtime choices

### External executable

This is the smallest integration technically, but every worker would need a pinned executable. SeaTunnel would also need contracts for command discovery, timeouts, process termination, temporary files, output limits, stderr handling, concurrency, and unavailable executables.

### WebAssembly

This avoids spawning one process per document, but SeaTunnel currently has no Java WebAssembly runtime boundary. Adding one requires a runtime selection, dependency and distribution review, memory and execution limits, and Linux/macOS/Windows verification.

### Sidecar service

This separates the native runtime from the JVM, but adds a network API, deployment, authentication, retry, and availability contract. It is not equivalent to an optional local parser.

## Decision required before production code

Maintainers need to choose the supported runtime and packaging model before user-facing options or a `DOCUMENT` format are added. The decision should define:

1. whether the first backend is an executable, WebAssembly runtime, or sidecar;
2. who installs and versions the backend on every worker;
3. supported operating systems and architectures;
4. timeout, output-size, resource, concurrency, and cleanup limits;
5. error categories for unsupported, encrypted, malformed, and image-only files;
6. whether GFM tables should preserve current paragraph behavior or become table elements;
7. licensing, NOTICE, binary-distribution, and vulnerability-update responsibilities.

After that decision, the production slice can add a document parser SPI, one optional provider, declarative options, EN/ZH connector documentation, and connector E2E coverage without changing the current PDF and Markdown defaults.
