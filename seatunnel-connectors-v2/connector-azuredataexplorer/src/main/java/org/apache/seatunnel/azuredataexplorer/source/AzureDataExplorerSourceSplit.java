package org.apache.seatunnel.azuredataexplorer.source;

import org.apache.seatunnel.api.source.SourceSplit;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serial;

@AllArgsConstructor
@Getter
public class AzureDataExplorerSourceSplit implements SourceSplit {
    @Serial private static final long serialVersionUID = 1L;

    private final String splitId;

    @Override
    public String splitId() {
        return splitId;
    }
}
