package org.apache.seatunnel.core.starter.utils;
import org.apache.seatunnel.api.configuration.ConfigShade;

public class AzureKeyVaultConfigShade implements ConfigShade {

    @Override
    public String getIdentifier() {
        return "azure-kv";
    }

    @Override
    public String encrypt(String content) {
        return content;
    }

    @Override
    public String decrypt(String content) {

        if (content == null) {
            return null;
        }

        if (content.startsWith("${keyvault:azure:")) {
            return content;
        }

        return content;
    }
}
