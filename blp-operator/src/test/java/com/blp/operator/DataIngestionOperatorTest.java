package com.blp.operator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@ExtendWith(MockitoExtension.class)
class DataIngestionOperatorTest {

    @InjectMocks DataIngestionOperator operator;

    String config =
            "\n"
                    + "{\n"
                    + "  \"env\": {\n"
                    + "    \"job.mode\": \"BATCH\"\n"
                    + "  },\n"
                    + "  \"source\": [\n"
                    + "    {\n"
                    + "      \"plugin_name\": \"FakeSource\",\n"
                    + "      \"result_table_name\": \"fake\",\n"
                    + "      \"row.num\": 100,\n"
                    + "      \"schema\": {\n"
                    + "        \"fields\": {\n"
                    + "          \"name\": \"string\",\n"
                    + "          \"age\": \"int\",\n"
                    + "          \"card\": \"int\"\n"
                    + "        }\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ],\n"
                    + "  \"sink\": [\n"
                    + "    {\n"
                    + "      \"plugin_name\": \"Console\",\n"
                    + "      \"source_table_name\": \"fake\"\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}\n";

    @Test
    void testPrepareSubmit() throws IOException {
        DataIngestionRequest request = new DataIngestionRequest();
        request.configJson = config;
        request.instanceName = "jn";
        String filename = "/tmp/config.json";
        Path path = Paths.get("/tmp/config.json");
        Boolean result = operator.prepareSubmit(request, filename);
        Assertions.assertTrue(result);
        Assertions.assertTrue(Files.exists(path));
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(path.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertEquals(content.toString(), config);
        Files.delete(path);
    }
}
