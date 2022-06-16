package org.apache.seatunnel.flink.assertion.rule;

import junit.framework.TestCase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import java.util.List;

public class AssertRuleParserTest extends TestCase {
    AssertRuleParser parser = new AssertRuleParser();
    public void testParseRules() {
        List<? extends Config> ruleConfigList = assembleConfig();
        List<AssertFieldRule> assertFieldRules = parser.parseRules(ruleConfigList);
        assertEquals(assertFieldRules.size(), 2);
        assertEquals(assertFieldRules.get(0).getFieldType(), BasicTypeInfo.STRING_TYPE_INFO);
    }
    private List<? extends Config> assembleConfig() {
        String s="AssertSink {\n" +
                "    rules = \n" +
                "        [{\n" +
                "            field_name = name\n" +
                "            field_type = string\n" +
                "            field_value = [\n" +
                "                {\n" +
                "                    rule_type = NOT_NULL\n" +
                "                },\n" +
                "                {\n" +
                "                    rule_type = MIN_LENGTH\n" +
                "                    rule_value = 3\n" +
                "                },\n" +
                "                {\n" +
                "                     rule_type = MAX_LENGTH\n" +
                "                     rule_value = 5\n" +
                "                }\n" +
                "            ]\n" +
                "        },{\n" +
                "            field_name = age\n" +
                "            field_type = int\n" +
                "            field_value = [\n" +
                "                {\n" +
                "                    rule_type = NOT_NULL\n" +
                "                },\n" +
                "                {\n" +
                "                    rule_type = MIN\n" +
                "                    rule_value = 10\n" +
                "                },\n" +
                "                {\n" +
                "                     rule_type = MAX\n" +
                "                     rule_value = 20\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "        ]\n" +
                "    \n" +
                "}\n";
        Config config = ConfigFactory.parseString(s);

        return config.getConfig("AssertSink").getConfigList("rules");
    }

}