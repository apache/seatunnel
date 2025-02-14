package org.apache.seatunnel.format.sensorsdata.record;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import lombok.Getter;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;

/**
 * @author chwang
 * @version 1.0.0
 */
public class UserEventRecord extends UserRecordBase {

    private String eventName;

    @Getter private UserEventSchema userEventSchema;

    public UserEventRecord(UserEventSchema userEventSchema) {
        super(
                userEventSchema.getTrackId(),
                userEventSchema.getDistinctId(),
                userEventSchema.getIdentityMap(),
                userEventSchema.getPropertyMap(),
                TRACK_ACTION_TYPE,
                SensorsConst.USER_EVENT_SCHEMA);
        this.userEventSchema = userEventSchema;
        this.eventName = userEventSchema.getEventName();
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> data = super.toMapWithOutProperties();
        addTimeFree(data);
        data.put(SensorsDataJsonKeys.EVENT, eventName);
        Map<String, Object> properties = this.userEventSchema.getPropertyMap();
        checkAndSetIdentity(properties);
        data.put(SensorsDataJsonKeys.PROPERTIES, properties);
        return data;
    }
}
