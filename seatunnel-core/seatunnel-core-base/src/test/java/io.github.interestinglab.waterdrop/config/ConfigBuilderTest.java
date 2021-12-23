package io.github.interestinglab.waterdrop.config;

import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

import static io.github.interestinglab.waterdrop.utils.Engine.SPARK;

public class ConfigBuilderTest {

    @Test
    public void defineVariableTest() {
        String configFile = System.getProperty("user.dir") + "/src/test/resources/spark.batch.test.conf";
        HashMap variableMap = new HashMap();
        Tuple2<String, String> date = new Tuple2("date", "20210101");
        variableMap = variableMap.$plus(date);
        ConfigBuilder configBuilder = new ConfigBuilder(configFile, SPARK, variableMap);
        Config config = configBuilder.getEnvConfigs();
        Assert.assertTrue(config.getString("spark.app.name").contains("20210101"));
        String resText = configBuilder.fileRegexExecution(configFile, variableMap);
        Assert.assertTrue(resText.contains("20210101"));

        // negative case
        HashMap emptyVariableMap = new HashMap();
        ConfigBuilder configBuilder1 = new ConfigBuilder(configFile, SPARK, emptyVariableMap);
        Config config1 = configBuilder1.getEnvConfigs();
        Assert.assertTrue(!config1.getString("spark.app.name").contains("20210101"));
        resText = configBuilder.fileRegexExecution(configFile, emptyVariableMap);
        Assert.assertTrue(!resText.contains("20210101"));
    }
}
