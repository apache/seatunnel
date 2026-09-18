# Update transform validation logic to use declarative constraints
import org.apache.seatunnel.api.configuration.OptionRule;
import org.apache.seatunnel.api.configuration.Conditions;

public class Transform {
    private OptionRule optionRule;

    public Transform() {
        optionRule = OptionRule.builder()
                .optional("minValue", "maxValue", Conditions.lessOrEqualField("minValue", "maxValue"))
                .build();
    }

    public boolean validate(Map<String, Object> config) {
        return optionRule.check(config);
    }
}