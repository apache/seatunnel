package org.apache.seatunnel.connectors.seatunnel.google.sheets.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class SheetsParameters implements Serializable {

    private byte[] serviceAccountKey;

    private String sheetId;

    private String sheetName;

    private String range;

    private List<String> headers;

    public SheetsParameters buildWithConfig(Config config) {
        this.serviceAccountKey = config.getString(SheetsConfig.SERVICE_ACCOUNT_KEY).getBytes();
        this.sheetId = config.getString(SheetsConfig.SHEET_ID);
        this.sheetName = config.getString(SheetsConfig.SHEET_NAME);
        this.range = config.getString(SheetsConfig.RANGE);
        this.headers = config.getStringList(SheetsConfig.HEADERS);
        return this;
    }

}
