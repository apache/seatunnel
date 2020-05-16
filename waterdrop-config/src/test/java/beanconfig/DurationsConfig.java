package beanconfig;

import java.time.Duration;

public class DurationsConfig {
    Duration second;
    Duration secondAsNumber;
    Duration halfSecond;


    public Duration getSecond() {
        return second;
    }

    public void setSecond(Duration second) {
        this.second = second;
    }

    public Duration getSecondAsNumber() {
        return secondAsNumber;
    }

    public void setSecondAsNumber(Duration secondAsNumber) {
        this.secondAsNumber = secondAsNumber;
    }

    public Duration getHalfSecond() {
        return halfSecond;
    }

    public void setHalfSecond(Duration halfSecond) {
        this.halfSecond = halfSecond;
    }
}
