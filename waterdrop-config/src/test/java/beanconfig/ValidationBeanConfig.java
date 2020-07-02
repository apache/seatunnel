package beanconfig;

import java.util.List;

public class ValidationBeanConfig extends TestBeanConfig{

    private String propNotListedInConfig;
    private int shouldBeInt;
    private boolean shouldBeBoolean;
    private List<Integer> shouldBeList;

    public String getPropNotListedInConfig() {
        return propNotListedInConfig;
    }

    public void setPropNotListedInConfig(String propNotListedInConfig) {
        this.propNotListedInConfig = propNotListedInConfig;
    }

    public int getShouldBeInt() {
        return shouldBeInt;
    }

    public void setShouldBeInt(int v) {
        shouldBeInt = v;
    }

    public boolean getShouldBeBoolean() {
        return shouldBeBoolean;
    }

    public void setShouldBeBoolean(boolean v) {
        shouldBeBoolean = v;
    }

    public List<Integer> getShouldBeList() {
        return shouldBeList;
    }

    public void setShouldBeList(List<Integer> v) {
        shouldBeList = v;
    }

}
