package org.apache.seatunnel.flink.assertion.rule;

import lombok.Data;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.List;
@Data
public class AssertFieldRule implements Serializable {
    private String fieldName;
    private TypeInformation<?> fieldType;
    private List<AssertValueRule> fieldValueRules;

    @Data
    public static class AssertValueRule implements Serializable {
        private AssertValueRuleType fieldValueRuleType;
        private Double fieldValueRuleValue;
    }

    public enum AssertValueRuleType {
        NOT_NULL,
        MIN,
        MAX,
        MIN_LENGTH,
        MAX_LENGTH
    }
}
