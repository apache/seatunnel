package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.api.configuration.ConfigShade;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

import java.util.Map;

public class AzureKeyVaultConfigShade implements ConfigShade {

    private static final String IDENTIFIER = "azure-kv";

    private SecretClient secretClient;

    @Override
    public void open(Map<String, Object> props) {
        Object vaultUrl = props.get("vault.url");

        if (vaultUrl == null || vaultUrl.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Missing vault.url");
        }

        this.secretClient =
                new SecretClientBuilder()
                        .vaultUrl(vaultUrl.toString())
                        .credential(new DefaultAzureCredentialBuilder().build())
                        .buildClient();
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
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

        if (content.startsWith("${keyvault:azure:") && content.endsWith("}")) {
            String secretName =
                    content
                            .replace("${keyvault:azure:", "")
                            .replace("}", "");

            int slashIndex = secretName.lastIndexOf("/");

            if (slashIndex >= 0) {
                secretName = secretName.substring(slashIndex + 1);
            }

            return secretClient.getSecret(secretName).getValue();
        }

        return content;
    }
}