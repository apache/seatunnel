package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.JsonReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class FileFilterPatternTest {
    /**
     * filter based on the file directory at the same time, the expression needs to start with
     * `path`
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void testJsonFilterPatternWithFilePath() throws URISyntaxException, IOException {
        URL filterPattern = FileFilterPatternTest.class.getResource("/filter-pattern/json");
        Assertions.assertNotNull(filterPattern);
        // path
        String jsonPathDir = filterPattern.toURI().getPath();
        // the expression needs to start with `path`
        String fileFilterPattern = jsonPathDir + "/json2025[^/]*/.*.json";
        Config pluginConfig =
                ConfigFactory.empty()
                        .withValue(
                                FileBaseSourceOptions.FILE_FILTER_PATTERN.key(),
                                ConfigValueFactory.fromAnyRef(fileFilterPattern));
        JsonReadStrategy jsonReadStrategy = new JsonReadStrategy();
        ExcelReadStrategyTest.LocalConf localConf =
                new ExcelReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        jsonReadStrategy.setPluginConfig(pluginConfig);
        jsonReadStrategy.init(localConf);

        List<String> filterFileNames = jsonReadStrategy.getFileNamesByPath(jsonPathDir);
        Assertions.assertEquals(1, filterFileNames.size());
        String fileName = filterFileNames.get(0);
        Assertions.assertTrue(fileName.endsWith(".json"));
    }

    /**
     * filter based on file names, just simply write the regular file names
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Test
    public void testJsonFilterPatternWithFileName() throws URISyntaxException, IOException {
        URL filterPattern = FileFilterPatternTest.class.getResource("/filter-pattern/json");
        Assertions.assertNotNull(filterPattern);
        // path
        String jsonPathDir = filterPattern.toURI().getPath();
        // just simply write the regular file names
        String fileFilterPattern = ".*.json";
        Config pluginConfig =
                ConfigFactory.empty()
                        .withValue(
                                FileBaseSourceOptions.FILE_FILTER_PATTERN.key(),
                                ConfigValueFactory.fromAnyRef(fileFilterPattern));
        JsonReadStrategy jsonReadStrategy = new JsonReadStrategy();
        ExcelReadStrategyTest.LocalConf localConf =
                new ExcelReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        jsonReadStrategy.setPluginConfig(pluginConfig);
        jsonReadStrategy.init(localConf);

        List<String> filterFileNames = jsonReadStrategy.getFileNamesByPath(jsonPathDir);
        Assertions.assertEquals(3, filterFileNames.size());
        for (String fileName : filterFileNames) {
            Assertions.assertTrue(fileName.endsWith(".json"));
        }
    }
}
