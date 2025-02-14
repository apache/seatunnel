package org.apache.seatunnel.format.sensorsdata.record;

import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import lombok.Getter;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.DETAIL_SET_ACTION_TYPE;

/**
 * @author chwang
 * @version 1.0.0
 */
public class UserDetailRecord extends UserRecordBase {

    @Getter private final DetailSchema userDetailSchema;

    public UserDetailRecord(DetailSchema userDetailSchema) {
        super(
                userDetailSchema.getTrackId(),
                userDetailSchema.getDistinctId(),
                userDetailSchema.getIdentities(),
                userDetailSchema.getProperties(),
                DETAIL_SET_ACTION_TYPE,
                userDetailSchema.getSchema());
        this.userDetailSchema = userDetailSchema;
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> data = super.toMapWithOutProperties();
        data.put(SensorsDataJsonKeys.ID, userDetailSchema.getDetailId());
        Map<String, Object> properties = this.userDetailSchema.getProperties();
        if (userDetailSchema.getItemPair() != null) {
            properties.put(
                    userDetailSchema.getItemPair().getKey(),
                    userDetailSchema.getItemPair().getValue());
        }
        if (!userDetailSchema.getIdentities().isEmpty()) {
            checkAndSetIdentity(properties);
        }
        data.put(SensorsDataJsonKeys.PROPERTIES, properties);
        return data;
    }
}
