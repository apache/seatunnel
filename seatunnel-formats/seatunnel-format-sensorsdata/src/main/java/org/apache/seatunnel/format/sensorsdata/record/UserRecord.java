package org.apache.seatunnel.format.sensorsdata.record;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import lombok.Getter;

import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ACTION_TYPE;

public class UserRecord extends UserRecordBase {

    @Getter private final UserSchema userSchema;

    public UserRecord(UserSchema userSchema) {
        this(userSchema, PROFILE_SET_ACTION_TYPE);
    }

    public UserRecord(UserSchema userSchema, String actionType) {
        super(
                userSchema.getTrackId(),
                userSchema.getDistinctId(),
                userSchema.getIdentityMap(),
                userSchema.getPropertyMap(),
                actionType,
                SensorsConst.USER_SCHEMA);
        this.userSchema = userSchema;
    }

    protected Map<String, Object> toMap() {
        Map<String, Object> data = super.toMapWithOutProperties();
        checkAndSetIdentity(data);
        data.put(SensorsDataJsonKeys.PROPERTIES, userSchema.getPropertyMap());
        return data;
    }
}
