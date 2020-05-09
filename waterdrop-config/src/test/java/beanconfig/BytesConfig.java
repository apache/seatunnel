package beanconfig;

import io.github.interestinglab.waterdrop.config.ConfigMemorySize;

public class BytesConfig {

    private ConfigMemorySize kilobyte;
    private ConfigMemorySize kibibyte;
    private ConfigMemorySize thousandBytes;

    public ConfigMemorySize getKilobyte() {
        return kilobyte;
    }

    public void setKilobyte(ConfigMemorySize kilobyte) {
        this.kilobyte = kilobyte;
    }

    public ConfigMemorySize getKibibyte() {
        return kibibyte;
    }

    public void setKibibyte(ConfigMemorySize kibibyte) {
        this.kibibyte = kibibyte;
    }

    public ConfigMemorySize getThousandBytes() {
        return thousandBytes;
    }

    public void setThousandBytes(ConfigMemorySize thousandBytes) {
        this.thousandBytes = thousandBytes;
    }
}
