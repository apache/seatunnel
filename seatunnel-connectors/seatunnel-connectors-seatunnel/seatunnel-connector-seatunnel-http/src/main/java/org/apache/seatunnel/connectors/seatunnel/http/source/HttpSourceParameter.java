package org.apache.seatunnel.connectors.seatunnel.http.source;

import static org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceParameter.ScheduleParameter.PERIOD_DEFAULT_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceParameter.ScheduleParameter.UNIT_DEFAULT_VALUE;

import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Data
public class HttpSourceParameter implements Serializable {

    private String url;
    private String method;
    private String headers;
    private String params;
    private String body;
    private String schedulePeriod;

    public Map<String, String> getHeadersMap() {
        return JsonUtils.toMap(this.headers);
    }

    public Map<String, String> getParamsMap() {
        return JsonUtils.toMap(this.params);
    }

    public ScheduleParameter toScheduleParameter() {
        if (StringUtils.isBlank(this.schedulePeriod)) {
            return ScheduleParameter.builder().period(PERIOD_DEFAULT_VALUE).unit(UNIT_DEFAULT_VALUE).build();
        }
        String[] scheduleParameter = this.schedulePeriod.split(" ");
        if (scheduleParameter.length > 2) {
            throw new IllegalArgumentException(MessageFormat.format("http source parameter: schedulePeriod parameter value [{0}] not supported.", this.schedulePeriod));
        }

        if (!NumberUtils.isNumber(scheduleParameter[0])) {
            throw new IllegalArgumentException(MessageFormat.format("http source parameter: schedulePeriod parameter value [{0}] not supported. schedule period needs to be a numeric type, e.g '10 SECONDS'.", this.schedulePeriod, scheduleParameter[1]));
        }
        Long period = Long.valueOf(scheduleParameter[0]);
        TimeUnit unit = UNIT_DEFAULT_VALUE;

        if (scheduleParameter.length == 2) {
            unit = EnumUtils.getEnum(TimeUnit.class, scheduleParameter[1]);
            if (unit == null) {
                throw new IllegalArgumentException(MessageFormat.format("http source parameter: schedulePeriod parameter value [{0}] not supported. schedule period time unit [{1}] does not exist, e.g '10 SECONDS'.", this.schedulePeriod, scheduleParameter[1]));
            }
        }
        return ScheduleParameter.builder()
                .period(period)
                .unit(unit)
                .build();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ScheduleParameter {
        public static final Long PERIOD_DEFAULT_VALUE = 5L;
        public static final TimeUnit UNIT_DEFAULT_VALUE = TimeUnit.SECONDS;
        private Long period;
        private TimeUnit unit;
    }
}
