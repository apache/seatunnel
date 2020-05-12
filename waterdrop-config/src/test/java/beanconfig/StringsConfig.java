package beanconfig;


public class StringsConfig {
    String abcd;
    String yes;

    public String getAbcd() {
        return abcd;
    }

    public void setAbcd(String s) {
        abcd = s;
    }

    public String getYes() {
        return yes;
    }

    public void setYes(String s) {
        yes = s;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StringsConfig) {
            StringsConfig sc = (StringsConfig) o;
            return sc.abcd.equals(abcd) &&
                sc.yes.equals(yes);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int h = 41 * (41 + abcd.hashCode());
        return h + yes.hashCode();
    }

    @Override
    public String toString() {
        return "StringsConfig(" + abcd + "," + yes + ")";
    }
}
