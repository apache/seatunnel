package org.apache.seatunnel.api.transform;

import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.api.common.SeaTunnelPluginLifeCycle;
import org.apache.seatunnel.api.source.SeaTunnelJobAware;

import java.io.Serializable;

public interface Transform
        extends Serializable,
        PluginIdentifierInterface,
        SeaTunnelPluginLifeCycle,
        SeaTunnelJobAware {
}
