# Update OptionRule test cases to use declarative constraints
import org.apache.seatunnel.api.configuration.OptionRule;
import org.apache.seatunnel.api.configuration.Conditions;
import org.junit.Test;

public class OptionRuleTest {
    @Test
    public void testRequired() {
        OptionRule optionRule = OptionRule.builder()
                .required("port", Conditions.greaterThan("port", 0))
                .build();
        Map<String, Object> config = new HashMap<>();
        config.put("port", 1);
        assertTrue(optionRule.check(config));
    }

    @Test
    public void testOptional() {
        OptionRule optionRule = OptionRule.builder()
                .optional("minValue", "maxValue", Conditions.lessOrEqualField("minValue", "maxValue"))
                .build();
        Map<String, Object> config = new HashMap<>();
        config.put("minValue", 1);
        config.put("maxValue", 2);
        assertTrue(optionRule.check(config));
    }

    @Test
    public void testConditional() {
        OptionRule optionRule = OptionRule.builder()
                .conditional("ignoreNoLeaderPartition", true, Conditions.greaterThan("partitionDiscoveryIntervalMillis", 0))
                .build();
        Map<String, Object> config = new HashMap<>();
        config.put("ignoreNoLeaderPartition", true);
        config.put("partitionDiscoveryIntervalMillis", 1);
        assertTrue(optionRule.check(config));
    }
}