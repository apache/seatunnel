package org.apache.seatunnel.transform.execption;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GroovyTransformErrorCode implements SeaTunnelErrorCode {

    // 重复命名
    TRANSFORMER_CONFIGURATION_ERROR("GROOVY_TRANSFORM-01", "Transformer configuration error"),
    TRANSFORMER_ILLEGAL_PARAMETER("GROOVY_TRANSFORM-02", "Transformer parameter illegal"),
    TRANSFORMER_RUN_EXCEPTION("GROOVY_TRANSFORM-03", "Transformer run exception"),
    TRANSFORMER_GROOVY_INIT_EXCEPTION("GROOVY_TRANSFORM-04", "Transformer Groovy init exception");

    private final String code;
    private final String description;

    GroovyTransformErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
