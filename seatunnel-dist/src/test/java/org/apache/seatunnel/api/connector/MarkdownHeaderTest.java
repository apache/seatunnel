package org.apache.seatunnel.api.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MarkdownHeaderTest {

    private static List<Path> docsDirectorys = new ArrayList<>();

    @BeforeAll
    public static void setup() {
        docsDirectorys.add(Paths.get("..", "docs", "en"));
        docsDirectorys.add(Paths.get("..", "docs", "zh"));
    }

    @Test
    public void testPrimaryHeadersHaveNoTextAbove() throws IOException {
        docsDirectorys.forEach(
                docsDirectory -> {
                    try (Stream<Path> paths = Files.walk(docsDirectory)) {
                        List<Path> mdFiles =
                                paths.filter(Files::isRegularFile)
                                        .filter(path -> path.toString().endsWith(".md"))
                                        .collect(Collectors.toList());

                        for (Path mdPath : mdFiles) {
                            List<String> lines = Files.readAllLines(mdPath, StandardCharsets.UTF_8);

                            String firstRelevantLine = null;
                            int lineNumber = 0;
                            boolean inFrontMatter = false;

                            for (int i = 0; i < lines.size(); i++) {
                                String line = lines.get(i).trim();
                                lineNumber = i + 1;

                                if (i == 0 && line.equals("---")) {
                                    inFrontMatter = true;
                                    continue;
                                }
                                if (inFrontMatter) {
                                    if (line.equals("---")) {
                                        inFrontMatter = false;
                                    }
                                    continue;
                                }

                                if (line.isEmpty()) {
                                    continue;
                                }

                                if (line.startsWith("import ")) {
                                    continue;
                                }

                                firstRelevantLine = line;
                                break;
                            }

                            if (firstRelevantLine == null) {
                                Assertions.fail(
                                        String.format(
                                                "The file %s is empty and has no content.",
                                                mdPath.toString()));
                            }

                            if (!firstRelevantLine.startsWith("# ")) {
                                Assertions.fail(
                                        String.format(
                                                "The first line of the file %s is not a first level heading. First line content: “%s” (line number: %d)",
                                                mdPath.toString(), firstRelevantLine, lineNumber));
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
